/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.searcher.Hits;
import org.apache.nutch.searcher.NutchBean;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.util.NutchConfiguration;

/**
 * <p>A simple tool to perform load testing on configured search servers.  A 
 * queries file can be specified with a list of different queries to run against
 * the search servers.  The number of threads used to perform concurrent
 * searches is also configurable.</p>
 * 
 * <p>This tool will output approximate times for running all queries in the 
 * queries file.  If configured it will also print out individual queries times
 * to the log.</p>
 */
public class SearchLoadTester {

  public static final Log LOG = LogFactory.getLog(SearchLoadTester.class);

  private String queriesFile = null;
  private int numThreads = 100;
  private boolean showTimes = false;
  private ExecutorService pool = null;
  private static AtomicInteger numTotal = new AtomicInteger(0);
  private static AtomicInteger numErrored = new AtomicInteger(0);
  private static AtomicInteger numResolved = new AtomicInteger(0);
  private static AtomicLong totalTime = new AtomicLong(0L);

  private static Configuration conf = null;
  private static NutchBean bean = null;

  private static class SearchThread
    extends Thread {

    private String query = null;
    private boolean showTimes = false;

    public SearchThread(String query, boolean showTimes) {
      this.query = query;
      this.showTimes = showTimes;
    }

    public void run() {

      numTotal.incrementAndGet();

      try {
        Query runner = Query.parse(query, conf);
        long start = System.currentTimeMillis();
        Hits hits = bean.search(runner, 10);
        long end = System.currentTimeMillis();
        numResolved.incrementAndGet();
        long total = (end - start);
        if (showTimes) {
          System.out.println("Query for " + query + " numhits "
            + hits.getTotal() + " in " + total + "ms");
        }
        totalTime.addAndGet(total);
      }
      catch (Exception uhe) {
        LOG.info("Error executing search for " + query);
        numErrored.incrementAndGet();
      }
    }
  }

  public void testSearch() {

    try {

      // create a thread pool with a fixed number of threads
      pool = Executors.newFixedThreadPool(numThreads);

      // read in the queries file and loop through each line, one query per line
      BufferedReader buffRead = new BufferedReader(new FileReader(new File(
        queriesFile)));
      String queryStr = null;
      while ((queryStr = buffRead.readLine()) != null) {
        pool.execute(new SearchThread(queryStr, showTimes));
      }

      // close the file and wait for up to 60 seconds before shutting down
      // the thread pool to give urls time to finish resolving
      buffRead.close();
      pool.shutdown();
      pool.awaitTermination(60, TimeUnit.SECONDS);

      LOG.info("Total Queries: " + numTotal.get() + ", Errored: "
        + numErrored.get() + ", Total Time: " + totalTime.get()
        + ", Average Time: " + totalTime.get() / numTotal.get()
        + " with " + numThreads + " threads");
    }
    catch (Exception e) {
      e.printStackTrace();
      // on error shutdown the thread pool immediately
      pool.shutdownNow();
      LOG.info(StringUtils.stringifyException(e));
    }

  }

  public SearchLoadTester(String queriesFile)
    throws IOException {
    this(queriesFile, 100, false);
  }

  public SearchLoadTester(String queriesFile, int numThreads, boolean showTimes)
    throws IOException {
    this.queriesFile = queriesFile;
    this.numThreads = numThreads;
    this.showTimes = showTimes;
    this.conf = NutchConfiguration.create();
    this.bean = new NutchBean(conf);
  }

  public static void main(String[] args) {

    Options options = new Options();
    Option helpOpts = OptionBuilder.withArgName("help").withDescription(
      "show this help message").create("help");
    Option queriesOpts = OptionBuilder.withArgName("queries").hasArg().withDescription(
      "the queries file to test").create("queries");
    Option numThreadOpts = OptionBuilder.withArgName("numThreads").hasArgs().withDescription(
      "the number of threads to use").create("numThreads");
    Option showTimesOpts = OptionBuilder.withArgName("showTimes").withDescription(
      "show individual query times").create("showTimes");
    options.addOption(helpOpts);
    options.addOption(queriesOpts);
    options.addOption(numThreadOpts);
    options.addOption(showTimesOpts);

    CommandLineParser parser = new GnuParser();
    try {

      // parse out common line arguments
      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("queries")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SearchTester", options);
        return;
      }

      // get the urls and the number of threads and start the resolver
      boolean showTimes = line.hasOption("showTimes");
      String queries = line.getOptionValue("queries");
      int numThreads = 10;
      String numThreadsStr = line.getOptionValue("numThreads");
      if (numThreadsStr != null) {
        numThreads = Integer.parseInt(numThreadsStr);
      }
      SearchLoadTester tester = new SearchLoadTester(queries, numThreads, showTimes);
      tester.testSearch();
    }
    catch (Exception e) {
      LOG.fatal("SearchTester: " + StringUtils.stringifyException(e));
    }
  }

}

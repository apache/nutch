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
import java.net.InetAddress;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.util.URLUtil;

/**
 * A simple tool that will spin up multiple threads to resolve urls to ip
 * addresses. This can be used to verify that pages that are failing due to
 * UnknownHostException during fetching are actually bad and are not failing due
 * to a dns problem in fetching.
 */
public class ResolveUrls {

  public static final Logger LOG = LoggerFactory.getLogger(ResolveUrls.class);

  private String urlsFile = null;
  private int numThreads = 100;
  private ExecutorService pool = null;
  private static AtomicInteger numTotal = new AtomicInteger(0);
  private static AtomicInteger numErrored = new AtomicInteger(0);
  private static AtomicInteger numResolved = new AtomicInteger(0);
  private static AtomicLong totalTime = new AtomicLong(0L);

  /**
   * A Thread which gets the ip address of a single host by name.
   */
  private static class ResolverThread
    extends Thread {

    private String url = null;

    public ResolverThread(String url) {
      this.url = url;
    }

    public void run() {

      numTotal.incrementAndGet();
      String host = URLUtil.getHost(url);
      long start = System.currentTimeMillis();
      try {
        // get the address by name and if no error is thrown then it 
        // is resolved successfully
        InetAddress.getByName(host);
        LOG.info("Resolved: " + host);
        numResolved.incrementAndGet();
      }
      catch (Exception uhe) {
        LOG.info("Error Resolving: " + host);
        numErrored.incrementAndGet();
      }
      long end = System.currentTimeMillis();
      long total = (end - start);
      totalTime.addAndGet(total);
      LOG.info(", " + total + " millis");
    }
  }

  /**
   * Creates a thread pool for resolving urls.  Reads in the url file on the
   * local filesystem.  For each url it attempts to resolve it keeping a total
   * account of the number resolved, errored, and the amount of time.
   */
  public void resolveUrls() {

    try {

      // create a thread pool with a fixed number of threads
      pool = Executors.newFixedThreadPool(numThreads);
      
      // read in the urls file and loop through each line, one url per line
      BufferedReader buffRead = new BufferedReader(new FileReader(new File(
        urlsFile)));
      String urlStr = null;
      while ((urlStr = buffRead.readLine()) != null) {
        
        // spin up a resolver thread per url
        LOG.info("Starting: " + urlStr);
        pool.execute(new ResolverThread(urlStr));
      }

      // close the file and wait for up to 60 seconds before shutting down
      // the thread pool to give urls time to finish resolving
      buffRead.close();
      pool.awaitTermination(60, TimeUnit.SECONDS);
    }
    catch (Exception e) {
      
      // on error shutdown the thread pool immediately
      pool.shutdownNow();
      LOG.info(StringUtils.stringifyException(e));
    }

    // shutdown the thread pool and log totals
    pool.shutdown();
    LOG.info("Total: " + numTotal.get() + ", Resovled: "
      + numResolved.get() + ", Errored: " + numErrored.get()
      + ", Average Time: " + totalTime.get() / numTotal.get());
  }

  /**
   * Create a new ResolveUrls with a file from the local file system.
   *
   * @param urlsFile The local urls file, one url per line.
   */
  public ResolveUrls(String urlsFile) {
    this(urlsFile, 100);
  }

  /**
   * Create a new ResolveUrls with a urls file and a number of threads for the
   * Thread pool.  Number of threads is 100 by default.
   * 
   * @param urlsFile The local urls file, one url per line.
   * @param numThreads The number of threads used to resolve urls in parallel.
   */
  public ResolveUrls(String urlsFile, int numThreads) {
    this.urlsFile = urlsFile;
    this.numThreads = numThreads;
  }

  /**
   * Runs the resolve urls tool.
   */
  public static void main(String[] args) {

    Options options = new Options();
    OptionBuilder.withArgName("help");
    OptionBuilder.withDescription("show this help message");
    Option helpOpts = OptionBuilder.create("help");
    
    OptionBuilder.withArgName("urls");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("the urls file to check");
    Option urlOpts = OptionBuilder.create("urls");
    
    OptionBuilder.withArgName("numThreads");
    OptionBuilder.hasArgs();
    OptionBuilder.withDescription("the number of threads to use");
    Option numThreadOpts = OptionBuilder.create("numThreads");
    
    options.addOption(helpOpts);
    options.addOption(urlOpts);
    options.addOption(numThreadOpts);

    CommandLineParser parser = new GnuParser();
    try {

      // parse out common line arguments
      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("urls")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ResolveUrls", options);
        return;
      }

      // get the urls and the number of threads and start the resolver
      String urls = line.getOptionValue("urls");
      int numThreads = 100;
      String numThreadsStr = line.getOptionValue("numThreads");
      if (numThreadsStr != null) {
        numThreads = Integer.parseInt(numThreadsStr);
      }
      ResolveUrls resolve = new ResolveUrls(urls, numThreads);
      resolve.resolveUrls();
    }
    catch (Exception e) {
      LOG.error("ResolveUrls: " + StringUtils.stringifyException(e));
    }
  }

}

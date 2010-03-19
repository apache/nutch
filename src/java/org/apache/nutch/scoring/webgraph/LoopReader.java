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
package org.apache.nutch.scoring.webgraph;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.nutch.scoring.webgraph.Loops.LoopSet;
import org.apache.nutch.util.FSUtils;
import org.apache.nutch.util.NutchConfiguration;

/**
 * The LoopReader tool prints the loopset information for a single url.
 */
public class LoopReader {

  private Configuration conf;
  private FileSystem fs;
  private MapFile.Reader[] loopReaders;

  /**
   * Prints loopset for a single url.  The loopset information will show any
   * outlink url the eventually forms a link cycle.
   * 
   * @param webGraphDb The WebGraph to check for loops
   * @param url The url to check.
   * 
   * @throws IOException If an error occurs while printing loopset information.
   */
  public void dumpUrl(Path webGraphDb, String url)
    throws IOException {

    // open the readers
    conf = NutchConfiguration.create();
    fs = FileSystem.get(conf);
    loopReaders = MapFileOutputFormat.getReaders(fs, new Path(webGraphDb,
      Loops.LOOPS_DIR), conf);

    // get the loopset for a given url, if any
    Text key = new Text(url);
    LoopSet loop = new LoopSet();
    MapFileOutputFormat.getEntry(loopReaders,
      new HashPartitioner<Text, LoopSet>(), key, loop);

    // print out each loop url in the set
    System.out.println(url + ":");
    for (String loopUrl : loop.getLoopSet()) {
      System.out.println("  " + loopUrl);
    }

    // close the readers
    FSUtils.closeReaders(loopReaders);
  }

  /**
   * Runs the LoopReader tool.  For this tool to work the loops job must have
   * already been run on the corresponding WebGraph.
   */
  public static void main(String[] args)
    throws Exception {

    Options options = new Options();
    Option helpOpts = OptionBuilder.withArgName("help").withDescription(
      "show this help message").create("help");
    Option webGraphOpts = OptionBuilder.withArgName("webgraphdb").hasArg()
      .withDescription("the webgraphdb to use").create("webgraphdb");
    Option urlOpts = OptionBuilder.withArgName("url").hasOptionalArg()
      .withDescription("the url to dump").create("url");
    options.addOption(helpOpts);
    options.addOption(webGraphOpts);
    options.addOption(urlOpts);

    CommandLineParser parser = new GnuParser();
    try {

      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("webgraphdb")
        || !line.hasOption("url")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("WebGraphReader", options);
        return;
      }

      String webGraphDb = line.getOptionValue("webgraphdb");
      String url = line.getOptionValue("url");
      LoopReader reader = new LoopReader();
      reader.dumpUrl(new Path(webGraphDb), url);
      return;
    }
    catch (Exception e) {
      e.printStackTrace();
      return;
    }
  }

}

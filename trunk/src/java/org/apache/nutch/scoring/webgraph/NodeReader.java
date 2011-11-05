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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.nutch.util.FSUtils;
import org.apache.nutch.util.NutchConfiguration;

/**
 * Reads and prints to system out information for a single node from the NodeDb 
 * in the WebGraph.
 */
public class NodeReader extends Configured {

  private FileSystem fs;
  private MapFile.Reader[] nodeReaders;

  public NodeReader() {
    
  }
  
  public NodeReader(Configuration conf) {
    super(conf);
  }
  
  /**
   * Prints the content of the Node represented by the url to system out.
   * 
   * @param webGraphDb The webgraph from which to get the node.
   * @param url The url of the node.
   * 
   * @throws IOException If an error occurs while getting the node.
   */
  public void dumpUrl(Path webGraphDb, String url)
    throws IOException {

    fs = FileSystem.get(getConf());
    nodeReaders = MapFileOutputFormat.getReaders(fs, new Path(webGraphDb,
      WebGraph.NODE_DIR), getConf());

    // open the readers, get the node, print out the info, and close the readers
    Text key = new Text(url);
    Node node = new Node();
    MapFileOutputFormat.getEntry(nodeReaders,
      new HashPartitioner<Text, Node>(), key, node);
    System.out.println(url + ":");
    System.out.println("  inlink score: " + node.getInlinkScore());
    System.out.println("  outlink score: " + node.getOutlinkScore());
    System.out.println("  num inlinks: " + node.getNumInlinks());
    System.out.println("  num outlinks: " + node.getNumOutlinks());
    FSUtils.closeReaders(nodeReaders);
  }

  /**
   * Runs the NodeReader tool.  The command line arguments must contain a 
   * webgraphdb path and a url.  The url must match the normalized url that is
   * contained in the NodeDb of the WebGraph.
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

      // command line must take a webgraphdb and a url
      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("webgraphdb")
        || !line.hasOption("url")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("WebGraphReader", options);
        return;
      }

      // dump the values to system out and return
      String webGraphDb = line.getOptionValue("webgraphdb");
      String url = line.getOptionValue("url");
      NodeReader reader = new NodeReader(NutchConfiguration.create());
      reader.dumpUrl(new Path(webGraphDb), url);
      
      return;
    }
    catch (Exception e) {
      e.printStackTrace();
      return;
    }
  }

}

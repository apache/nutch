/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.crawl;

import java.io.IOException;
import java.io.File;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.conf.Configuration;

import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

import java.util.Iterator;
import java.util.logging.Logger;

/** . */
public class LinkDbReader implements Closeable {
  public static final Logger LOG = LogFormatter.getLogger(LinkDbReader.class.getName());

  private static final Partitioner PARTITIONER = new HashPartitioner();

  private FileSystem fs;
  private File directory;
  private MapFile.Reader[] readers;
  private Configuration conf;

  public LinkDbReader(FileSystem fs, File directory, Configuration conf) {
    this.fs = fs;
    this.directory = directory;
    this.conf = conf;
  }

  public String[] getAnchors(UTF8 url) throws IOException {
    Inlinks inlinks = getInlinks(url);
    if (inlinks == null)
      return null;
    return inlinks.getAnchors();
  }

  public Inlinks getInlinks(UTF8 url) throws IOException {

    synchronized (this) {
      if (readers == null) {
        readers = MapFileOutputFormat.getReaders
          (fs, new File(directory, LinkDb.CURRENT_NAME), this.conf);
      }
    }
    
    return (Inlinks)MapFileOutputFormat.getEntry
      (readers, PARTITIONER, url, new Inlinks());
  }
  
  public void close() throws IOException {
    if (readers != null) {
      for (int i = 0; i < readers.length; i++) {
        readers[i].close();
      }
    }
  }
  
  public static void processDumpJob(String linkdb, String output, Configuration config) throws IOException {
    LOG.info("LinkDb dump: starting");
    LOG.info("LinkDb db: " + linkdb);
    File outFolder = new File(output);

    JobConf job = new NutchJob(config);

    job.addInputDir(new File(linkdb, LinkDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(Inlinks.class);

    job.setOutputDir(outFolder);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(Inlinks.class);

    JobClient.runJob(job);
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("LinkDbReader <linkdb> {-dump <out_dir> | -url <url>)");
      System.err.println("\t-dump <out_dir>\tdump whole link db to a text file in <out_dir>");
      System.err.println("\t-url <url>\tprint information about <url> to System.out");
      return;
    }
    Configuration conf = NutchConfiguration.create();
    if (args[1].equals("-dump")) {
      LinkDbReader.processDumpJob(args[0], args[2], conf);
    } else if (args[1].equals("-url")) {
      LinkDbReader dbr = new LinkDbReader(FileSystem.get(NutchConfiguration.create()), new File(args[0]), conf);
      Inlinks links = dbr.getInlinks(new UTF8(args[2]));
      if (links == null) {
        System.out.println(" - no link information.");
      } else {
        Iterator it = links.iterator();
        while (it.hasNext()) {
          System.out.println(it.next().toString());
        }
      }
    } else {
      System.err.println("Error: wrong argument " + args[1]);
      return;
    }
  }
}

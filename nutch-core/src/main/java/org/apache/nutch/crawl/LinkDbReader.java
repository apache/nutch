/**
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

package org.apache.nutch.crawl;

import java.io.IOException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;

import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.io.Closeable;

/** . */
public class LinkDbReader extends Configured implements Tool, Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(LinkDbReader.class);

  private static final Partitioner<WritableComparable, Writable> PARTITIONER = new HashPartitioner<WritableComparable, Writable>();

  private FileSystem fs;
  private Path directory;
  private MapFile.Reader[] readers;

  public LinkDbReader() {

  }

  public LinkDbReader(Configuration conf, Path directory) throws Exception {
    setConf(conf);
    init(directory);
  }

  public void init(Path directory) throws Exception {
    this.fs = FileSystem.get(getConf());
    this.directory = directory;
  }

  public String[] getAnchors(Text url) throws IOException {
    Inlinks inlinks = getInlinks(url);
    if (inlinks == null)
      return null;
    return inlinks.getAnchors();
  }

  public Inlinks getInlinks(Text url) throws IOException {

    if (readers == null) {
      synchronized (this) {
        readers = MapFileOutputFormat.getReaders(fs, new Path(directory,
            LinkDb.CURRENT_NAME), getConf());
      }
    }

    return (Inlinks) MapFileOutputFormat.getEntry(readers, PARTITIONER, url,
        new Inlinks());
  }

  public void close() throws IOException {
    if (readers != null) {
      for (int i = 0; i < readers.length; i++) {
        readers[i].close();
      }
    }
  }
  
  public static class LinkDBDumpMapper implements Mapper<Text, Inlinks, Text, Inlinks> {
    Pattern pattern = null;
    Matcher matcher = null;
    
    public void configure(JobConf job) {
      if (job.get("linkdb.regex", null) != null) {
        pattern = Pattern.compile(job.get("linkdb.regex"));
      }
    }

    public void close() {}
    public void map(Text key, Inlinks value, OutputCollector<Text, Inlinks> output, Reporter reporter)
            throws IOException {

      if (pattern != null) {
        matcher = pattern.matcher(key.toString());
        if (!matcher.matches()) {
          return;
        }
      }

      output.collect(key, value);
    }
  }

  public void processDumpJob(String linkdb, String output, String regex) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("LinkDb dump: starting at " + sdf.format(start));
      LOG.info("LinkDb dump: db: " + linkdb);
    }
    Path outFolder = new Path(output);

    JobConf job = new NutchJob(getConf());
    job.setJobName("read " + linkdb);
    
    if (regex != null) {
      job.set("linkdb.regex", regex);
      job.setMapperClass(LinkDBDumpMapper.class);
    }

    FileInputFormat.addInputPath(job, new Path(linkdb, LinkDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);

    FileOutputFormat.setOutputPath(job, outFolder);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Inlinks.class);

    JobClient.runJob(job);

    long end = System.currentTimeMillis();
    LOG.info("LinkDb dump: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new LinkDbReader(),
        args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err
          .println("Usage: LinkDbReader <linkdb> (-dump <out_dir> [-regex <regex>]) | -url <url>");
      System.err
          .println("\t-dump <out_dir>\tdump whole link db to a text file in <out_dir>");
      System.err
          .println("\t\t-regex <regex>\trestrict to url's matching expression");
      System.err
          .println("\t-url <url>\tprint information about <url> to System.out");
      return -1;
    }
    try {
      if (args[1].equals("-dump")) {
        String regex = null;
        for (int i = 2; i < args.length; i++) {
          if (args[i].equals("-regex")) {
            regex = args[++i];
          }
        }
        processDumpJob(args[0], args[2], regex);
        return 0;
      } else if (args[1].equals("-url")) {
        init(new Path(args[0]));
        Inlinks links = getInlinks(new Text(args[2]));
        if (links == null) {
          System.out.println(" - no link information.");
        } else {
          Iterator<Inlink> it = links.iterator();
          while (it.hasNext()) {
            System.out.println(it.next().toString());
          }
        }
        return 0;
      } else {
        System.err.println("Error: wrong argument " + args[1]);
        return -1;
      }
    } catch (Exception e) {
      LOG.error("LinkDbReader: " + StringUtils.stringifyException(e));
      return -1;
    }
  }
}

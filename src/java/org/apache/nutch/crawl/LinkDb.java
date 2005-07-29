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

import java.io.*;
import java.util.*;
import java.util.logging.*;

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.net.*;
import org.apache.nutch.util.*;
import org.apache.nutch.mapred.*;
import org.apache.nutch.parse.*;

/** Maintains an inverted link map, listing incoming links for each url. */
public class LinkDb extends NutchConfigured implements Mapper, Reducer {

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.crawl.LinkDb");

  public static String CURRENT_NAME = "current";

  private int maxAnchorLength;

  public LinkDb() {
    super(null);
  }

  /** Construct an LinkDb. */
  public LinkDb(NutchConf conf) {
    super(conf);
  }

  public void configure(JobConf job) {
    maxAnchorLength = job.getInt("db.max.anchor.length", 100);
  }

  public void map(WritableComparable key, Writable value,
                  OutputCollector output, Reporter reporter)
    throws IOException {
    String fromUrl = key.toString();
    ParseData parseData = (ParseData)value;
    Outlink[] outlinks = parseData.getOutlinks();
    Inlinks inlinks = new Inlinks();
    for (int i = 0; i < outlinks.length; i++) {
      Outlink outlink = outlinks[i];
      inlinks.clear();
      String anchor = outlink.getAnchor();        // truncate long anchors
      if (anchor.length() > maxAnchorLength) {
        anchor = anchor.substring(0, maxAnchorLength);
      }
      inlinks.add(new Inlink(fromUrl, anchor));   // collect inverted link
      output.collect(new UTF8(outlink.getToUrl()), inlinks);
    }
  }

  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter)
    throws IOException {
    Inlinks result = null;
    while (values.hasNext()) {
      Inlinks inlinks = (Inlinks)values.next();
      if (result == null) {
        result = inlinks;
      } else {
        result.add(inlinks);
      }
    }
    output.collect(key, result);
  }


  public void invert(File linkDb, File segmentsDir) throws IOException {
    LOG.info("LinkDb: starting");
    LOG.info("LinkDb: linkdb: " + linkDb);
    LOG.info("LinkDb: segments: " + segmentsDir);

    JobConf job = LinkDb.createJob(getConf(), linkDb);
    job.setInputDir(segmentsDir);
    job.set("mapred.input.subdir", ParseData.DIR_NAME);
    JobClient.runJob(job);
    LinkDb.install(job, linkDb);
    LOG.info("LinkDb: done");
  }

  public void invert(File linkDb, File[] segments) throws IOException {
    LOG.info("LinkDb: starting");
    LOG.info("LinkDb: linkdb: " + linkDb);
    JobConf job = LinkDb.createJob(getConf(), linkDb);
    for (int i = 0; i < segments.length; i++) {
      LOG.info("LinkDb: adding segment: " + segments[i]);
      job.addInputDir(new File(segments[i], ParseData.DIR_NAME));
    }
    JobClient.runJob(job);
    LinkDb.install(job, linkDb);
    LOG.info("LinkDb: done");
  }

  private static JobConf createJob(NutchConf config, File linkDb) {
    File newLinkDb =
      new File(linkDb,
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new JobConf(config);

    job.setInt("partition.url.by.host.seed", new Random().nextInt());
    job.setPartitionerClass(PartitionUrlByHost.class);

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(ParseData.class);

    job.setMapperClass(LinkDb.class);
    //job.setCombinerClass(LinkDb.class);
    job.setReducerClass(LinkDb.class);

    job.setOutputDir(newLinkDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(Inlinks.class);

    return job;
  }

  public static void install(JobConf job, File linkDb) throws IOException {
    File newLinkDb = job.getOutputDir();
    NutchFileSystem fs = new JobClient(job).getFs();
    File old = new File(linkDb, "old");
    File current = new File(linkDb, CURRENT_NAME);
    fs.delete(old);
    fs.rename(current, old);
    fs.rename(newLinkDb, current);
    fs.delete(old);
  }

  public static void main(String[] args) throws Exception {
    LinkDb linkDb = new LinkDb(NutchConf.get());
    
    if (args.length < 2) {
      System.err.println("Usage: <linkdb> <segments>");
      return;
    }
    
    linkDb.invert(new File(args[0]), new File(args[1]));
  }



}

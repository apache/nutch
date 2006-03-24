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
import java.net.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.mapred.*;

import org.apache.nutch.parse.*;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/** Maintains an inverted link map, listing incoming links for each url. */
public class LinkDb extends Configured implements Mapper, Reducer {

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.crawl.LinkDb");

  public static String CURRENT_NAME = "current";

  private int maxAnchorLength;
  private int maxInlinks;
  private boolean ignoreInternalLinks;

  public LinkDb() {
    super(null);
  }

  /** Construct an LinkDb. */
  public LinkDb(Configuration conf) {
    super(conf);
  }

  public void configure(JobConf job) {
    maxAnchorLength = job.getInt("db.max.anchor.length", 100);
    maxInlinks = job.getInt("db.max.inlinks", 10000);
    ignoreInternalLinks = job.getBoolean("db.ignore.internal.links", true);
  }

  public void close() {}

  public void map(WritableComparable key, Writable value,
                  OutputCollector output, Reporter reporter)
    throws IOException {
    String fromUrl = key.toString();
    String fromHost = getHost(fromUrl);

    ParseData parseData = (ParseData)value;
    Outlink[] outlinks = parseData.getOutlinks();
    Inlinks inlinks = new Inlinks();
    for (int i = 0; i < outlinks.length; i++) {
      Outlink outlink = outlinks[i];
      String toUrl = outlink.getToUrl();

      if (ignoreInternalLinks) {
        String toHost = getHost(toUrl);
        if (toHost == null || toHost.equals(fromHost)) { // internal link
          continue;                               // skip it
        }
      }

      inlinks.clear();
      String anchor = outlink.getAnchor();        // truncate long anchors
      if (anchor.length() > maxAnchorLength) {
        anchor = anchor.substring(0, maxAnchorLength);
      }
      inlinks.add(new Inlink(fromUrl, anchor));   // collect inverted link
      output.collect(new UTF8(toUrl), inlinks);
    }
  }

  private String getHost(String url) {
    try {
      return new URL(url).getHost().toLowerCase();
    } catch (MalformedURLException e) {
      return null;
    }
  }

  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter)
    throws IOException {

    Inlinks result = null;

    while (values.hasNext()) {
      Inlinks inlinks = (Inlinks)values.next();

      if (result == null) {                       // optimize a common case
        if (inlinks.size() < maxInlinks) {
          result = inlinks;
          continue;
        } else {
          result = new Inlinks();
        }
      }

      int end = Math.min(maxInlinks - result.size(), inlinks.size());
      Iterator it = inlinks.iterator();
      int i = 0;
      while(it.hasNext() && i++ < end) {
        result.add((Inlink)it.next());
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

  private static JobConf createJob(Configuration config, File linkDb) {
    File newLinkDb =
      new File(linkDb,
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.setJobName("linkdb " + linkDb);

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(ParseData.class);

    job.setMapperClass(LinkDb.class);
    //job.setCombinerClass(LinkDb.class);
    job.setReducerClass(LinkDb.class);

    job.setOutputDir(newLinkDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setBoolean("mapred.output.compress", true);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(Inlinks.class);

    return job;
  }

  public static void install(JobConf job, File linkDb) throws IOException {
    File newLinkDb = job.getOutputDir();
    FileSystem fs = new JobClient(job).getFs();
    File old = new File(linkDb, "old");
    File current = new File(linkDb, CURRENT_NAME);
    fs.delete(old);
    fs.rename(current, old);
    fs.rename(newLinkDb, current);
    fs.delete(old);
  }

  public static void main(String[] args) throws Exception {
    LinkDb linkDb = new LinkDb(NutchConfiguration.create());
    
    if (args.length < 2) {
      System.err.println("Usage: <linkdb> (-dir segmentsDir | segment1 segment2 ...)");
      return;
    }
    boolean dir = false;
    File segDir = null;
    File db = new File(args[0]);
    ArrayList segs = new ArrayList();
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-dir")) {
        dir = true;
        segDir = new File(args[++i]);
        break;
      } else segs.add(new File(args[i]));
    }
    if (dir) linkDb.invert(db, segDir);
    else linkDb.invert(db, (File[])segs.toArray(new File[segs.size()]));
  }



}

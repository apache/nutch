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
import java.net.*;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;

import org.apache.nutch.net.URLFilters;
import org.apache.nutch.parse.*;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/** Maintains an inverted link map, listing incoming links for each url. */
public class LinkDb extends Configured implements Mapper, Reducer {

  public static final Log LOG = LogFactory.getLog(LinkDb.class);

  public static String CURRENT_NAME = "current";

  private int maxAnchorLength;
  private int maxInlinks;
  private boolean ignoreInternalLinks;
  
  public static class Merger extends MapReduceBase implements Reducer {
    private int _maxInlinks;
    private URLFilters filters = null;
    
    public void configure(JobConf job) {
      super.configure(job);
      _maxInlinks = job.getInt("db.max.inlinks", 10000);
      if (job.getBoolean("linkdb.merger.urlfilters", false)) {
        filters = new URLFilters(job);
      }
    }

    public void reduce(WritableComparable key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
      if (filters != null) {
        try {
          if (filters.filter(((UTF8)key).toString()) == null)
            return;
        } catch (Exception e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Can't filter " + key + ": " + e);
          }
        }
      }
      Inlinks inlinks = null;
      while (values.hasNext()) {
        if (inlinks == null) {
          inlinks = (Inlinks)values.next();
          continue;
        }
        Inlinks val = (Inlinks)values.next();
        for (Iterator it = val.iterator(); it.hasNext(); ) {
          if (inlinks.size() >= _maxInlinks) {
            output.collect(key, inlinks);
            return;
          }
          Inlink in = (Inlink)it.next();
          if (filters != null) {
            try {
              if (filters.filter(in.getFromUrl()) == null)
                continue;
            } catch (Exception e) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Can't filter " + key + ": " + e);
              }
            }
          }
          inlinks.add(in);
        }
      }
      if (inlinks.size() == 0) return;
      output.collect(key, inlinks);
    }
  }

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

  public void invert(Path linkDb, final Path segmentsDir) throws IOException {
    final FileSystem fs = FileSystem.get(getConf());
    Path[] files = fs.listPaths(segmentsDir, new PathFilter() {
      public boolean accept(Path f) {
        try {
          if (fs.isDirectory(f)) return true;
        } catch (IOException ioe) {};
        return false;
      }
    });
    invert(linkDb, files);
  }

  public void invert(Path linkDb, Path[] segments) throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("LinkDb: starting");
      LOG.info("LinkDb: linkdb: " + linkDb);
    }
    JobConf job = LinkDb.createJob(getConf(), linkDb);
    for (int i = 0; i < segments.length; i++) {
      if (LOG.isInfoEnabled()) {
        LOG.info("LinkDb: adding segment: " + segments[i]);
      }
      job.addInputPath(new Path(segments[i], ParseData.DIR_NAME));
    }
    JobClient.runJob(job);
    FileSystem fs = FileSystem.get(getConf());
    if (fs.exists(linkDb)) {
      if (LOG.isInfoEnabled()) {
        LOG.info("LinkDb: merging with existing linkdb: " + linkDb);
      }
      // try to merge
      Path newLinkDb = job.getOutputPath();
      job = LinkDb.createMergeJob(getConf(), linkDb);
      job.addInputPath(new Path(linkDb, CURRENT_NAME));
      job.addInputPath(newLinkDb);
      JobClient.runJob(job);
      fs.delete(newLinkDb);
    }
    LinkDb.install(job, linkDb);
    if (LOG.isInfoEnabled()) { LOG.info("LinkDb: done"); }
  }

  private static JobConf createJob(Configuration config, Path linkDb) {
    Path newLinkDb =
      new Path("linkdb-" +
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.setJobName("linkdb " + linkDb);

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(ParseData.class);

    job.setMapperClass(LinkDb.class);
    job.setReducerClass(LinkDb.class);

    job.setOutputPath(newLinkDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setBoolean("mapred.output.compress", true);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(Inlinks.class);

    return job;
  }

  public static JobConf createMergeJob(Configuration config, Path linkDb) {
    Path newLinkDb =
      new Path("linkdb-merge-" + 
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.setJobName("linkdb merge " + linkDb);

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(Inlinks.class);

    job.setReducerClass(Merger.class);

    job.setOutputPath(newLinkDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setBoolean("mapred.output.compress", true);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(Inlinks.class);

    return job;
  }

  public static void install(JobConf job, Path linkDb) throws IOException {
    Path newLinkDb = job.getOutputPath();
    FileSystem fs = new JobClient(job).getFs();
    Path old = new Path(linkDb, "old");
    Path current = new Path(linkDb, CURRENT_NAME);
    if (fs.exists(current)) {
      if (fs.exists(old)) fs.delete(old);
      fs.rename(current, old);
    }
    fs.mkdirs(linkDb);
    fs.rename(newLinkDb, current);
    if (fs.exists(old)) fs.delete(old);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = NutchConfiguration.create();
    LinkDb linkDb = new LinkDb(conf);
    
    if (args.length < 2) {
      System.err.println("Usage: <linkdb> (-dir segmentsDir | segment1 segment2 ...)");
      return;
    }
    Path segDir = null;
    final FileSystem fs = FileSystem.get(conf);
    Path db = new Path(args[0]);
    ArrayList segs = new ArrayList();
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-dir")) {
        segDir = new Path(args[++i]);
        Path[] files = fs.listPaths(segDir, new PathFilter() {
          public boolean accept(Path f) {
            try {
              if (fs.isDirectory(f)) return true;
            } catch (IOException ioe) {};
            return false;
          }
        });
        if (files != null) segs.addAll(Arrays.asList(files));
        break;
      } else segs.add(new Path(args[i]));
    }
    linkDb.invert(db, (Path[])segs.toArray(new Path[segs.size()]));
  }



}

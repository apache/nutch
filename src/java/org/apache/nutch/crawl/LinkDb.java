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
import org.apache.hadoop.util.*;

import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.*;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/** Maintains an inverted link map, listing incoming links for each url. */
public class LinkDb extends Configured implements Tool, Mapper<Text, ParseData, Text, Inlinks> {

  public static final Log LOG = LogFactory.getLog(LinkDb.class);

  public static final String CURRENT_NAME = "current";
  public static final String LOCK_NAME = ".locked";

  private int maxAnchorLength;
  private boolean ignoreInternalLinks;
  private URLFilters urlFilters;
  private URLNormalizers urlNormalizers;
  
  public LinkDb() {}
  
  public LinkDb(Configuration conf) {
    setConf(conf);
  }
  
  public void configure(JobConf job) {
    maxAnchorLength = job.getInt("db.max.anchor.length", 100);
    ignoreInternalLinks = job.getBoolean("db.ignore.internal.links", true);
    if (job.getBoolean(LinkDbFilter.URL_FILTERING, false)) {
      urlFilters = new URLFilters(job);
    }
    if (job.getBoolean(LinkDbFilter.URL_NORMALIZING, false)) {
      urlNormalizers = new URLNormalizers(job, URLNormalizers.SCOPE_LINKDB);
    }
  }

  public void close() {}

  public void map(Text key, ParseData parseData,
                  OutputCollector<Text, Inlinks> output, Reporter reporter)
    throws IOException {
    String fromUrl = key.toString();
    String fromHost = getHost(fromUrl);
    if (urlNormalizers != null) {
      try {
        fromUrl = urlNormalizers.normalize(fromUrl, URLNormalizers.SCOPE_LINKDB); // normalize the url
      } catch (Exception e) {
        LOG.warn("Skipping " + fromUrl + ":" + e);
        fromUrl = null;
      }
    }
    if (fromUrl != null && urlFilters != null) {
      try {
        fromUrl = urlFilters.filter(fromUrl); // filter the url
      } catch (Exception e) {
        LOG.warn("Skipping " + fromUrl + ":" + e);
        fromUrl = null;
      }
    }
    if (fromUrl == null) return; // discard all outlinks
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
      if (urlNormalizers != null) {
        try {
          toUrl = urlNormalizers.normalize(toUrl, URLNormalizers.SCOPE_LINKDB); // normalize the url
        } catch (Exception e) {
          LOG.warn("Skipping " + toUrl + ":" + e);
          toUrl = null;
        }
      }
      if (toUrl != null && urlFilters != null) {
        try {
          toUrl = urlFilters.filter(toUrl); // filter the url
        } catch (Exception e) {
          LOG.warn("Skipping " + toUrl + ":" + e);
          toUrl = null;
        }
      }
      if (toUrl == null) continue;
      inlinks.clear();
      String anchor = outlink.getAnchor();        // truncate long anchors
      if (anchor.length() > maxAnchorLength) {
        anchor = anchor.substring(0, maxAnchorLength);
      }
      inlinks.add(new Inlink(fromUrl, anchor));   // collect inverted link
      output.collect(new Text(toUrl), inlinks);
    }
  }

  private String getHost(String url) {
    try {
      return new URL(url).getHost().toLowerCase();
    } catch (MalformedURLException e) {
      return null;
    }
  }

  public void invert(Path linkDb, final Path segmentsDir, boolean normalize, boolean filter, boolean force) throws IOException {
    final FileSystem fs = FileSystem.get(getConf());
    FileStatus[] files = fs.listStatus(segmentsDir, HadoopFSUtil.getPassDirectoriesFilter(fs));
    invert(linkDb, HadoopFSUtil.getPaths(files), normalize, filter, force);
  }

  public void invert(Path linkDb, Path[] segments, boolean normalize, boolean filter, boolean force) throws IOException {

    Path lock = new Path(linkDb, LOCK_NAME);
    FileSystem fs = FileSystem.get(getConf());
    LockUtil.createLockFile(fs, lock, force);
    Path currentLinkDb = new Path(linkDb, CURRENT_NAME);
    if (LOG.isInfoEnabled()) {
      LOG.info("LinkDb: starting");
      LOG.info("LinkDb: linkdb: " + linkDb);
      LOG.info("LinkDb: URL normalize: " + normalize);
      LOG.info("LinkDb: URL filter: " + filter);
    }
    JobConf job = LinkDb.createJob(getConf(), linkDb, normalize, filter);
    for (int i = 0; i < segments.length; i++) {
      if (LOG.isInfoEnabled()) {
        LOG.info("LinkDb: adding segment: " + segments[i]);
      }
      job.addInputPath(new Path(segments[i], ParseData.DIR_NAME));
    }
    try {
      JobClient.runJob(job);
    } catch (IOException e) {
      LockUtil.removeLockFile(fs, lock);
      throw e;
    }
    if (fs.exists(currentLinkDb)) {
      if (LOG.isInfoEnabled()) {
        LOG.info("LinkDb: merging with existing linkdb: " + linkDb);
      }
      // try to merge
      Path newLinkDb = job.getOutputPath();
      job = LinkDbMerger.createMergeJob(getConf(), linkDb, normalize, filter);
      job.addInputPath(currentLinkDb);
      job.addInputPath(newLinkDb);
      try {
        JobClient.runJob(job);
      } catch (IOException e) {
        LockUtil.removeLockFile(fs, lock);
        fs.delete(newLinkDb);
        throw e;
      }
      fs.delete(newLinkDb);
    }
    LinkDb.install(job, linkDb);
    if (LOG.isInfoEnabled()) { LOG.info("LinkDb: done"); }
  }

  private static JobConf createJob(Configuration config, Path linkDb, boolean normalize, boolean filter) {
    Path newLinkDb =
      new Path("linkdb-" +
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.setJobName("linkdb " + linkDb);

    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(LinkDb.class);
    job.setCombinerClass(LinkDbMerger.class);
    // if we don't run the mergeJob, perform normalization/filtering now
    if (normalize || filter) {
      try {
        FileSystem fs = FileSystem.get(config);
        if (!fs.exists(linkDb)) {
          job.setBoolean(LinkDbFilter.URL_FILTERING, filter);
          job.setBoolean(LinkDbFilter.URL_NORMALIZING, normalize);
        }
      } catch (Exception e) {
        LOG.warn("LinkDb createJob: " + e);
      }
    }
    job.setReducerClass(LinkDbMerger.class);

    job.setOutputPath(newLinkDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setBoolean("mapred.output.compress", true);
    job.setOutputKeyClass(Text.class);
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
    LockUtil.removeLockFile(fs, new Path(linkDb, LOCK_NAME));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new LinkDb(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: LinkDb <linkdb> (-dir <segmentsDir> | <seg1> <seg2> ...) [-force] [-noNormalize] [-noFilter]");
      System.err.println("\tlinkdb\toutput LinkDb to create or update");
      System.err.println("\t-dir segmentsDir\tparent directory of several segments, OR");
      System.err.println("\tseg1 seg2 ...\t list of segment directories");
      System.err.println("\t-force\tforce update even if LinkDb appears to be locked (CAUTION advised)");
      System.err.println("\t-noNormalize\tdon't normalize link URLs");
      System.err.println("\t-noFilter\tdon't apply URLFilters to link URLs");
      return -1;
    }
    Path segDir = null;
    final FileSystem fs = FileSystem.get(getConf());
    Path db = new Path(args[0]);
    ArrayList<Path> segs = new ArrayList<Path>();
    boolean filter = true;
    boolean normalize = true;
    boolean force = false;
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-dir")) {
        segDir = new Path(args[++i]);
        FileStatus[] files = fs.listStatus(segDir, HadoopFSUtil.getPassDirectoriesFilter(fs));
        if (files != null) segs.addAll(Arrays.asList(HadoopFSUtil.getPaths(files)));
        break;
      } else if (args[i].equalsIgnoreCase("-noNormalize")) {
        normalize = false;
      } else if (args[i].equalsIgnoreCase("-noFilter")) {
        filter = false;
      } else if (args[i].equalsIgnoreCase("-force")) {
        force = true;
      } else segs.add(new Path(args[i]));
    }
    try {
      invert(db, segs.toArray(new Path[segs.size()]), normalize, filter, force);
      return 0;
    } catch (Exception e) {
      LOG.fatal("LinkDb: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

}

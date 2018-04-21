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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TimingUtil;

/** Maintains an inverted link map, listing incoming links for each url. */
public class LinkDb extends NutchTool implements Tool {

  private static final Logger LOG = LoggerFactory
          .getLogger(MethodHandles.lookup().lookupClass());

  public static final String IGNORE_INTERNAL_LINKS = "linkdb.ignore.internal.links";
  public static final String IGNORE_EXTERNAL_LINKS = "linkdb.ignore.external.links";

  public static final String CURRENT_NAME = "current";
  public static final String LOCK_NAME = ".locked";

  public LinkDb() {
    //default constructor
  }

  public LinkDb(Configuration conf) {
    setConf(conf);
  }

  public static class LinkDbMapper extends 
  Mapper<Text, ParseData, Text, Inlinks> {
    private int maxAnchorLength;
    private boolean ignoreInternalLinks;
    private boolean ignoreExternalLinks;
    private URLFilters urlFilters;
    private URLNormalizers urlNormalizers;

    public void setup(Mapper<Text, ParseData, Text, Inlinks>.Context context) {
      Configuration conf = context.getConfiguration();
      maxAnchorLength = conf.getInt("linkdb.max.anchor.length", 100);
      ignoreInternalLinks = conf.getBoolean(IGNORE_INTERNAL_LINKS, true);
      ignoreExternalLinks = conf.getBoolean(IGNORE_EXTERNAL_LINKS, false);

      if (conf.getBoolean(LinkDbFilter.URL_FILTERING, false)) {
        urlFilters = new URLFilters(conf);
      }
      if (conf.getBoolean(LinkDbFilter.URL_NORMALIZING, false)) {
        urlNormalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_LINKDB);
      }
    } 

    public void cleanup(){
    }

    public void map(Text key, ParseData parseData,
            Context context)
                    throws IOException, InterruptedException {
      String fromUrl = key.toString();
      String fromHost = getHost(fromUrl);
      if (urlNormalizers != null) {
        try {
          fromUrl = urlNormalizers
                  .normalize(fromUrl, URLNormalizers.SCOPE_LINKDB); // normalize the url
        } catch (Exception e) {
          LOG.warn("Skipping {} :", fromUrl, e);
          fromUrl = null;
        }
      }
      if (fromUrl != null && urlFilters != null) {
        try {
          fromUrl = urlFilters.filter(fromUrl); // filter the url
        } catch (Exception e) {
          LOG.warn("Skipping {} :", fromUrl, e);
          fromUrl = null;
        }
      }
      if (fromUrl == null)
        return; // discard all outlinks
      Outlink[] outlinks = parseData.getOutlinks();
      Inlinks inlinks = new Inlinks();
      for (int i = 0; i < outlinks.length; i++) {
        Outlink outlink = outlinks[i];
        String toUrl = outlink.getToUrl();

        if (ignoreInternalLinks) {
          String toHost = getHost(toUrl);
          if (toHost == null || toHost.equals(fromHost)) { // internal link
            continue; // skip it
          }
        } else if (ignoreExternalLinks) {
          String toHost = getHost(toUrl);
          if (toHost == null || !toHost.equals(fromHost)) { // external link skip it
            continue;
          }
        }
        if (urlNormalizers != null) {
          try {
            // normalize the url
            toUrl = urlNormalizers.normalize(toUrl, URLNormalizers.SCOPE_LINKDB); 
          } catch (Exception e) {
            LOG.warn("Skipping {} :", toUrl, e);
            toUrl = null;
          }
        }
        if (toUrl != null && urlFilters != null) {
          try {
            toUrl = urlFilters.filter(toUrl); // filter the url
          } catch (Exception e) {
            LOG.warn("Skipping {} :", toUrl, e);
            toUrl = null;
          }
        }
        if (toUrl == null)
          continue;
        inlinks.clear();
        String anchor = outlink.getAnchor(); // truncate long anchors
        if (anchor.length() > maxAnchorLength) {
          anchor = anchor.substring(0, maxAnchorLength);
        }
        inlinks.add(new Inlink(fromUrl, anchor)); // collect inverted link
        context.write(new Text(toUrl), inlinks);
      }
    }
  }

  private static String getHost(String url) {
    try {
      return new URL(url).getHost().toLowerCase();
    } catch (MalformedURLException e) {
      return null;
    }
  }

  public void invert(Path linkDb, final Path segmentsDir, boolean normalize,
          boolean filter, boolean force) throws IOException, InterruptedException, ClassNotFoundException {
    FileSystem fs = segmentsDir.getFileSystem(getConf());
    FileStatus[] files = fs.listStatus(segmentsDir,
            HadoopFSUtil.getPassDirectoriesFilter(fs));
    invert(linkDb, HadoopFSUtil.getPaths(files), normalize, filter, force);
  }

  public void invert(Path linkDb, Path[] segments, boolean normalize,
          boolean filter, boolean force) throws IOException, InterruptedException, ClassNotFoundException {
    Job job = LinkDb.createJob(getConf(), linkDb, normalize, filter);
    Path lock = new Path(linkDb, LOCK_NAME);
    FileSystem fs = linkDb.getFileSystem(getConf());
    LockUtil.createLockFile(fs, lock, force);
    Path currentLinkDb = new Path(linkDb, CURRENT_NAME);
    Configuration conf = job.getConfiguration();

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("LinkDb: starting at {}", sdf.format(start));
      LOG.info("LinkDb: linkdb: {}", linkDb);
      LOG.info("LinkDb: URL normalize: {}", normalize);
      LOG.info("LinkDb: URL filter: {}", filter);
      if (conf.getBoolean(IGNORE_INTERNAL_LINKS, true)) {
        LOG.info("LinkDb: internal links will be ignored.");
      }
      if (conf.getBoolean(IGNORE_EXTERNAL_LINKS, false)) {
        LOG.info("LinkDb: external links will be ignored.");
      }
    }
    if (conf.getBoolean(IGNORE_INTERNAL_LINKS, true)
            && conf.getBoolean(IGNORE_EXTERNAL_LINKS, false)) {
      LOG.warn("LinkDb: internal and external links are ignored! "
              + "Nothing to do, actually. Exiting.");
      LockUtil.removeLockFile(fs, lock);
      return;
    }

    for (int i = 0; i < segments.length; i++) {
      if (LOG.isInfoEnabled()) {
        LOG.info("LinkDb: adding segment: {}", segments[i]);
      }
      FileInputFormat.addInputPath(job, new Path(segments[i],
              ParseData.DIR_NAME));
    }
    try {
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "LinkDb job did not succeed, job status:"
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        LockUtil.removeLockFile(fs, lock);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("LinkDb job failed: {}", e.getMessage());
      LockUtil.removeLockFile(fs, lock);
      throw e;
    }

    if (fs.exists(currentLinkDb)) {
      if (LOG.isInfoEnabled()) {
        LOG.info("LinkDb: merging with existing linkdb: {}", linkDb);
      }
      // try to merge
      Path newLinkDb = FileOutputFormat.getOutputPath(job);
      job = LinkDbMerger.createMergeJob(getConf(), linkDb, normalize, filter);
      FileInputFormat.addInputPath(job, currentLinkDb);
      FileInputFormat.addInputPath(job, newLinkDb);
      try {
        boolean success = job.waitForCompletion(true);
        if (!success) {
          String message = "LinkDb job did not succeed, job status:"
              + job.getStatus().getState() + ", reason: "
              + job.getStatus().getFailureInfo();
          LOG.error(message);
          NutchJob.cleanupAfterFailure(newLinkDb, lock, fs);
          throw new RuntimeException(message);
        }
      } catch (IOException | InterruptedException | ClassNotFoundException e) {
        LOG.error("LinkDb job failed: {}", e.getMessage());
        NutchJob.cleanupAfterFailure(newLinkDb, lock, fs);
        throw e;
      }
      fs.delete(newLinkDb, true);
    }
    LinkDb.install(job, linkDb);

    long end = System.currentTimeMillis();
    LOG.info("LinkDb: finished at {}, elapsed: {}", sdf.format(end), TimingUtil.elapsedTime(start, end));
  }

  private static Job createJob(Configuration config, Path linkDb,
          boolean normalize, boolean filter) throws IOException {
    Path newLinkDb = new Path(linkDb,
            Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    Job job = NutchJob.getInstance(config);
    Configuration conf = job.getConfiguration();
    job.setJobName("linkdb " + linkDb);

    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setJarByClass(LinkDb.class);
    job.setMapperClass(LinkDb.LinkDbMapper.class);

    job.setJarByClass(LinkDbMerger.class);
    job.setCombinerClass(LinkDbMerger.LinkDbMergeReducer.class);
    // if we don't run the mergeJob, perform normalization/filtering now
    if (normalize || filter) {
      try {
        FileSystem fs = linkDb.getFileSystem(config);
        if (!fs.exists(linkDb)) {
          conf.setBoolean(LinkDbFilter.URL_FILTERING, filter);
          conf.setBoolean(LinkDbFilter.URL_NORMALIZING, normalize);
        }
      } catch (Exception e) {
        LOG.warn("LinkDb createJob:: {}", e.getMessage());
      }
    }
    job.setReducerClass(LinkDbMerger.LinkDbMergeReducer.class);

    FileOutputFormat.setOutputPath(job, newLinkDb);
    job.setOutputFormatClass(MapFileOutputFormat.class);
    conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Inlinks.class);

    return job;
  }

  public static void install(Job job, Path linkDb) throws IOException {
    Configuration conf = job.getConfiguration();
    Path newLinkDb = FileOutputFormat.getOutputPath(job);
    FileSystem fs = linkDb.getFileSystem(conf);
    Path old = new Path(linkDb, "old");
    Path current = new Path(linkDb, CURRENT_NAME);
    if (fs.exists(current)) {
      if (fs.exists(old))
        fs.delete(old, true);
      fs.rename(current, old);
    }
    fs.mkdirs(linkDb);
    fs.rename(newLinkDb, current);
    if (fs.exists(old))
      fs.delete(old, true);
    LockUtil.removeLockFile(fs, new Path(linkDb, LOCK_NAME));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new LinkDb(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err
      .println("Usage: LinkDb <linkdb> (-dir <segmentsDir> | <seg1> <seg2> ...) [-force] [-noNormalize] [-noFilter]");
      System.err.println("\tlinkdb\toutput LinkDb to create or update");
      System.err
      .println("\t-dir segmentsDir\tparent directory of several segments, OR");
      System.err.println("\tseg1 seg2 ...\t list of segment directories");
      System.err
      .println("\t-force\tforce update even if LinkDb appears to be locked (CAUTION advised)");
      System.err.println("\t-noNormalize\tdon't normalize link URLs");
      System.err.println("\t-noFilter\tdon't apply URLFilters to link URLs");
      return -1;
    }
    Path db = new Path(args[0]);
    ArrayList<Path> segs = new ArrayList<>();
    boolean filter = true;
    boolean normalize = true;
    boolean force = false;
    for (int i = 1; i < args.length; i++) {
      if ("-dir".equals(args[i])) {
        Path segDir = new Path(args[++i]);
        FileSystem fs = segDir.getFileSystem(getConf());
        FileStatus[] paths = fs.listStatus(segDir,
                HadoopFSUtil.getPassDirectoriesFilter(fs));
        segs.addAll(Arrays.asList(HadoopFSUtil.getPaths(paths)));
      } else if ("-noNormalize".equalsIgnoreCase(args[i])) {
        normalize = false;
      } else if ("-noFilter".equalsIgnoreCase(args[i])) {
        filter = false;
      } else if ("-force".equalsIgnoreCase(args[i])) {
        force = true;
      } else
        segs.add(new Path(args[i]));
    }
    try {
      invert(db, segs.toArray(new Path[segs.size()]), normalize, filter, force);
      return 0;
    } catch (Exception e) {
      LOG.error("LinkDb: {}", StringUtils.stringifyException(e));
      return -1;
    }
  }

  /*
   * Used for Nutch REST service
   */
  @Override
  public Map<String, Object> run(Map<String, Object> args, String crawlId) throws Exception {

    Map<String, Object> results = new HashMap<>();

    Path linkdb;
    if(args.containsKey(Nutch.ARG_LINKDB)) {
      Object path = args.get(Nutch.ARG_LINKDB);
      if(path instanceof Path) {
        linkdb = (Path) path;
      }
      else {
        linkdb = new Path(path.toString());
      }
    }
    else {
      linkdb = new Path(crawlId+"/linkdb");
    }


    ArrayList<Path> segs = new ArrayList<>();
    boolean filter = true;
    boolean normalize = true;
    boolean force = false;
    if (args.containsKey("noNormalize")) {
      normalize = false;
    } 
    if (args.containsKey("noFilter")) {
      filter = false;
    } 
    if (args.containsKey("force")) {
      force = true;
    }

    Path segmentsDir;
    if(args.containsKey(Nutch.ARG_SEGMENTDIR)) {
      Object segDir = args.get(Nutch.ARG_SEGMENTDIR);
      if(segDir instanceof Path) {
        segmentsDir = (Path) segDir;
      }
      else {
        segmentsDir = new Path(segDir.toString());
      }
      FileSystem fs = segmentsDir.getFileSystem(getConf());
      FileStatus[] paths = fs.listStatus(segmentsDir,
              HadoopFSUtil.getPassDirectoriesFilter(fs));
      segs.addAll(Arrays.asList(HadoopFSUtil.getPaths(paths)));
    }
    else if(args.containsKey(Nutch.ARG_SEGMENTS)) {
      Object segments = args.get(Nutch.ARG_SEGMENTS);
      ArrayList<String> segmentList = new ArrayList<>(); 
      if(segments instanceof ArrayList) {
        segmentList = (ArrayList<String>)segments; }
      else if(segments instanceof Path){
        segmentList.add(segments.toString());
      }

      for(String segment: segmentList) {
        segs.add(new Path(segment));
      }
    }
    else {
      String segmentDir = crawlId+"/segments";
      File dir = new File(segmentDir);
      File[] segmentsList = dir.listFiles();  
      Arrays.sort(segmentsList, (f1, f2) -> {
        if(f1.lastModified()>f2.lastModified())
          return -1;
        else
          return 0;
      });
      segs.add(new Path(segmentsList[0].getPath()));
    }
    try {
      invert(linkdb, segs.toArray(new Path[segs.size()]), normalize, filter, force);
      results.put(Nutch.VAL_RESULT, Integer.toString(0));
      return results;
    } catch (Exception e) {
      LOG.error("LinkDb: {}", StringUtils.stringifyException(e));
      results.put(Nutch.VAL_RESULT, Integer.toString(-1));
      return results;
    }
  }
}

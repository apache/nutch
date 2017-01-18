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
package org.apache.nutch.indexer;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.segment.SegmentChecker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic indexer which relies on the plugins implementing IndexWriter
 **/

public class IndexingJob extends NutchTool implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public IndexingJob() {
    super(null);
  }

  public IndexingJob(Configuration conf) {
    super(conf);
  }

  public void index(Path crawlDb, Path linkDb, List<Path> segments,
      boolean noCommit) throws IOException {
    index(crawlDb, linkDb, segments, noCommit, false, null);
  }

  public void index(Path crawlDb, Path linkDb, List<Path> segments,
      boolean noCommit, boolean deleteGone) throws IOException {
    index(crawlDb, linkDb, segments, noCommit, deleteGone, null);
  }

  public void index(Path crawlDb, Path linkDb, List<Path> segments,
      boolean noCommit, boolean deleteGone, String params) throws IOException {
    index(crawlDb, linkDb, segments, noCommit, deleteGone, params, false, false);
  }

  public void index(Path crawlDb, Path linkDb, List<Path> segments,
      boolean noCommit, boolean deleteGone, String params, boolean filter,
      boolean normalize) throws IOException {
    index(crawlDb, linkDb, segments, noCommit, deleteGone, params, false,
        false, false);
  }

  public void index(Path crawlDb, Path linkDb, List<Path> segments,
      boolean noCommit, boolean deleteGone, String params,
      boolean filter, boolean normalize, boolean addBinaryContent) throws IOException {
    index(crawlDb, linkDb, segments, noCommit, deleteGone, params, false,
        false, false, false);
  }

  public void index(Path crawlDb, Path linkDb, List<Path> segments,
      boolean noCommit, boolean deleteGone, String params,
      boolean filter, boolean normalize, boolean addBinaryContent,
      boolean base64) throws IOException {


    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("Indexer: starting at {}", sdf.format(start));

    final JobConf job = new NutchJob(getConf());
    job.setJobName("Indexer");

    LOG.info("Indexer: deleting gone documents: {}", deleteGone);
    LOG.info("Indexer: URL filtering: {}", filter);
    LOG.info("Indexer: URL normalizing: {}", normalize);
    if (addBinaryContent) {
      if (base64) {
        LOG.info("Indexer: adding binary content as Base64");
      } else {
        LOG.info("Indexer: adding binary content");
      }
    }        
    IndexWriters writers = new IndexWriters(getConf());
    LOG.info(writers.describe());

    IndexerMapReduce.initMRJob(crawlDb, linkDb, segments, job, addBinaryContent);

    // NOW PASSED ON THE COMMAND LINE AS A HADOOP PARAM
    // job.set(SolrConstants.SERVER_URL, solrUrl);

    job.setBoolean(IndexerMapReduce.INDEXER_DELETE, deleteGone);
    job.setBoolean(IndexerMapReduce.URL_FILTERING, filter);
    job.setBoolean(IndexerMapReduce.URL_NORMALIZING, normalize);
    job.setBoolean(IndexerMapReduce.INDEXER_BINARY_AS_BASE64, base64);

    if (params != null) {
      job.set(IndexerMapReduce.INDEXER_PARAMS, params);
    }

    job.setReduceSpeculativeExecution(false);

    final Path tmp = new Path("tmp_" + System.currentTimeMillis() + "-"
        + new Random().nextInt());

    FileOutputFormat.setOutputPath(job, tmp);
    try {
      RunningJob indexJob = JobClient.runJob(job);
      // do the commits once and for all the reducers in one go
      if (!noCommit) {
        writers.open(job, "commit");
        writers.commit();
      }
      LOG.info("Indexer: number of documents indexed, deleted, or skipped:");
      for (Counter counter : indexJob.getCounters().getGroup("IndexerStatus")) {
        LOG.info("Indexer: {}  {}",
            String.format(Locale.ROOT, "%6d", counter.getValue()),
            counter.getName());
      }
      long end = System.currentTimeMillis();
      LOG.info("Indexer: finished at " + sdf.format(end) + ", elapsed: "
          + TimingUtil.elapsedTime(start, end));
    } finally {
      FileSystem.get(job).delete(tmp, true);
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err
      //.println("Usage: Indexer <crawldb> [-linkdb <linkdb>] [-params k1=v1&k2=v2...] (<segment> ... | -dir <segments>) [-noCommit] [-deleteGone] [-filter] [-normalize]");
      .println("Usage: Indexer <crawldb> [-linkdb <linkdb>] [-params k1=v1&k2=v2...] (<segment> ... | -dir <segments>) [-noCommit] [-deleteGone] [-filter] [-normalize] [-addBinaryContent] [-base64]");
      IndexWriters writers = new IndexWriters(getConf());
      System.err.println(writers.describe());
      return -1;
    }

    final Path crawlDb = new Path(args[0]);
    Path linkDb = null;

    final List<Path> segments = new ArrayList<Path>();
    String params = null;

    boolean noCommit = false;
    boolean deleteGone = false;
    boolean filter = false;
    boolean normalize = false;
    boolean addBinaryContent = false;
    boolean base64 = false;

    for (int i = 1; i < args.length; i++) {
      FileSystem fs = null;
      Path dir = null;
      if (args[i].equals("-linkdb")) {
        linkDb = new Path(args[++i]);
      } else if (args[i].equals("-dir")) {
        dir = new Path(args[++i]);
        fs = dir.getFileSystem(getConf());
        FileStatus[] fstats = fs.listStatus(dir,
            HadoopFSUtil.getPassDirectoriesFilter(fs));
        Path[] files = HadoopFSUtil.getPaths(fstats);
        for (Path p : files) {
          if (SegmentChecker.isIndexable(p,fs)) {
            segments.add(p);
          }
        }
      } else if (args[i].equals("-noCommit")) {
        noCommit = true;
      } else if (args[i].equals("-deleteGone")) {
        deleteGone = true;
      } else if (args[i].equals("-filter")) {
        filter = true;
      } else if (args[i].equals("-normalize")) {
        normalize = true;
      } else if (args[i].equals("-addBinaryContent")) {
        addBinaryContent = true;
      } else if (args[i].equals("-base64")) {
        base64 = true;
      } else if (args[i].equals("-params")) {
        params = args[++i];
      } else {
        dir = new Path(args[i]);
        fs = dir.getFileSystem(getConf());
        if (SegmentChecker.isIndexable(dir,fs)) {
          segments.add(dir);
        }
      }
    }

    try {
      index(crawlDb, linkDb, segments, noCommit, deleteGone, params, filter, normalize, addBinaryContent, base64);
      return 0;
    } catch (final Exception e) {
      LOG.error("Indexer: {}", StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new IndexingJob(), args);
    System.exit(res);
  }


  //Used for REST API
  @Override
  public Map<String, Object> run(Map<String, Object> args, String crawlId) throws Exception {
    boolean noCommit = false;
    boolean deleteGone = false; 
    boolean filter = false;
    boolean normalize = false;
    boolean isSegment = false;
    String params= null;
    Configuration conf = getConf();

    Path crawlDb;
    if(args.containsKey(Nutch.ARG_CRAWLDB)) {
      Object crawldbPath = args.get(Nutch.ARG_CRAWLDB);
      if(crawldbPath instanceof Path) {
        crawlDb = (Path) crawldbPath;
      }
      else {
        crawlDb = new Path(crawldbPath.toString());
      }
    }
    else {
      crawlDb = new Path(crawlId+"/crawldb");
    }

    Path linkdb = null;
    List<Path> segments = new ArrayList<Path>();

    if(args.containsKey(Nutch.ARG_LINKDB)){
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
    }

    if(args.containsKey(Nutch.ARG_SEGMENTDIR)){
      isSegment = true;
      Path segmentsDir;
      Object segDir = args.get(Nutch.ARG_SEGMENTDIR);
      if(segDir instanceof Path) {
        segmentsDir = (Path) segDir;
      }
      else {
        segmentsDir = new Path(segDir.toString());
      }
      FileSystem fs = segmentsDir.getFileSystem(getConf());
      FileStatus[] fstats = fs.listStatus(segmentsDir,
          HadoopFSUtil.getPassDirectoriesFilter(fs));
      Path[] files = HadoopFSUtil.getPaths(fstats);
      for (Path p : files) {
        if (SegmentChecker.isIndexable(p,fs)) {
          segments.add(p);
        }
      }     
    }

    if(args.containsKey(Nutch.ARG_SEGMENT)){
      isSegment = true;
      Object seg = args.get(Nutch.ARG_SEGMENT);
      ArrayList<String> segmentList = new ArrayList<String>();
      if(seg instanceof ArrayList) {
        segmentList = (ArrayList<String>)seg;
      }
      for(String segment: segmentList) {
        segments.add(new Path(segment));
      }
    }

    if(!isSegment){
      String segment_dir = crawlId+"/segments";
      File segmentsDir = new File(segment_dir);
      File[] segmentsList = segmentsDir.listFiles();  
      Arrays.sort(segmentsList, new Comparator<File>(){
        @Override
        public int compare(File f1, File f2) {
          if(f1.lastModified()>f2.lastModified())
            return -1;
          else
            return 0;
        }      
      });
      Path segment = new Path(segmentsList[0].getPath());
      segments.add(segment);
    }

    if(args.containsKey("noCommit")){
      noCommit = true;
    }
    if(args.containsKey("deleteGone")){
      deleteGone = true;
    }
    if(args.containsKey("normalize")){
      normalize = true;
    }
    if(args.containsKey("filter")){
      filter = true;
    }
    if(args.containsKey("params")){
      params = (String)args.get("params");
    }
    setConf(conf);
    index(crawlDb, linkdb, segments, noCommit, deleteGone, params, filter,
        normalize);
    Map<String, Object> results = new HashMap<String, Object>();
    results.put(Nutch.VAL_RESULT, 0);
    return results;
  }
}

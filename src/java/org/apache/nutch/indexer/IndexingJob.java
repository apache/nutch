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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
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

  public static Logger LOG = LoggerFactory.getLogger(IndexingJob.class);

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
      LOG.info("Indexer: finished at {}, elapsed: {}", sdf.format(end),
          TimingUtil.elapsedTime(start, end));
    } finally {
      FileSystem.get(job).delete(tmp, true);
    }
  }

  public int run(String[] args) throws Exception {
    // boolean options
    Option helpOpt = new Option("h", "help", false, "show this help message");
    // argument options
    @SuppressWarnings("static-access")
    Option crawldbOpt = OptionBuilder
    .withArgName("crawldb")
    .hasArg()
    .withDescription(
        "a crawldb directory to use with this tool (optional)")
    .create("crawldb");
    @SuppressWarnings("static-access")
    Option linkdbOpt = OptionBuilder
    .withArgName("linkdb")
    .hasArg()
    .withDescription(
        "a linkdb directory to use with this tool (optional)")
    .create("linkdb");
    @SuppressWarnings("static-access")
    Option paramsOpt = OptionBuilder
    .withArgName("params")
    .hasArg()
    .withDescription(
        "key value parameters to be used with this tool e.g. k1=v1&k2=v2... (optional)")
    .create("params");
    @SuppressWarnings("static-access")
    Option segOpt = OptionBuilder
    .withArgName("segment")
    .hasArgs()
    .withDescription("the segment(s) to use (either this or --segmentDir is mandatory)")
    .create("segment");
    @SuppressWarnings("static-access")
    Option segmentDirOpt = OptionBuilder
    .withArgName("segmentDir")
    .hasArg()
    .withDescription(
        "directory containing one or more segments to be used with this tool "
            + "(either this or --segment is mandatory)")
    .create("segmentDir");
    @SuppressWarnings("static-access")
    Option noCommitOpt = OptionBuilder
    .withArgName("noCommit")
    .withDescription(
        "do the commits once and for all the reducers in one go (optional)")
    .create("noCommit");
    @SuppressWarnings("static-access")
    Option deleteGoneOpt = OptionBuilder
    .withArgName("deleteGone")
    .withDescription(
        "delete gone documents e.g. documents which no longer exist at the particular resource (optional)")
    .create("deleteGone");
    @SuppressWarnings("static-access")
    Option filterOpt = OptionBuilder
    .withArgName("filter")
    .withDescription(
        "filter documents (optional)")
    .create("filter");
    @SuppressWarnings("static-access")
    Option normalizeOpt = OptionBuilder
    .withArgName("normalize")
    .withDescription(
        "normalize documents (optional)")
    .create("normalize");
    @SuppressWarnings("static-access")
    Option addBinaryContentOpt = OptionBuilder
    .withArgName("addBinaryContent")
    .withDescription(
        "add the raw content of the document to the indexing job (optional)")
    .create("addBinaryContent");
    @SuppressWarnings("static-access")
    Option base64Opt = OptionBuilder
    .withArgName("base64")
    .withDescription(
        "if raw content is added, base64 encode it (optional)")
    .create("base64");

    Options options = new Options();
    options.addOption(helpOpt);
    options.addOption(crawldbOpt);
    options.addOption(linkdbOpt);
    options.addOption(segOpt);
    options.addOption(paramsOpt);
    options.addOption(segmentDirOpt);
    options.addOption(noCommitOpt);
    options.addOption(deleteGoneOpt);
    options.addOption(filterOpt);
    options.addOption(normalizeOpt);
    options.addOption(addBinaryContentOpt);
    options.addOption(base64Opt);

    GnuParser parser = new GnuParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
      // if 'help' is present OR one of 'segmentDir' or 'segment' is NOT present then print help
      if (cmd.hasOption("help") || (!cmd.hasOption("segmentDir")
          && (!cmd.hasOption("segment")))) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(getClass().getSimpleName(), options, true);
        IndexWriters writers = new IndexWriters(getConf());
        LOG.error(writers.describe());
        return -1;
      }

      Path crawlDb = null;
      Path linkDb = null;
      final List<Path> segments = new ArrayList<Path>();
      String params = null;
      boolean noCommit = false;
      boolean deleteGone = false;
      boolean filter = false;
      boolean normalize = false;
      boolean addBinaryContent = false;
      boolean base64 = false;
      FileSystem fs = null;
      Path dir = null;
      if (cmd.hasOption("crawldb")) {
        crawlDb = new Path(cmd.getOptionValue("crawldb"));
      }
      if (cmd.hasOption("linkdb")) {
        linkDb = new Path(cmd.getOptionValue("linkdb"));
      }
      if (cmd.hasOption("params")) {
        params = cmd.getOptionValue("params");
      }
      if (cmd.hasOption("segment")) {
        dir = new Path(cmd.getOptionValue("segment"));
        fs = dir.getFileSystem(getConf());
        if (SegmentChecker.isIndexable(dir,fs)) {
          segments.add(dir);
        }
      } else if (cmd.hasOption("segmentDir")) {
        dir = new Path(cmd.getOptionValue("segmentDir"));
        fs = dir.getFileSystem(getConf());
        FileStatus[] fstats = fs.listStatus(dir,
            HadoopFSUtil.getPassDirectoriesFilter(fs));
        Path[] files = HadoopFSUtil.getPaths(fstats);
        for (Path p : files) {
          if (SegmentChecker.isIndexable(p,fs)) {
            segments.add(p);
          }
        }
      }
      if (cmd.hasOption("noCommit")) {
        noCommit = true;
      }
      if (cmd.hasOption("deleteGone")) {
        deleteGone = true;
      }
      if (cmd.hasOption("filter")) {
        filter = true;
      }
      if (cmd.hasOption("normalize")) {
        normalize = true;
      }
      if (cmd.hasOption("addBinaryContent")) {
        addBinaryContent = true;
      }
      if (cmd.hasOption("base64")) {
        base64 = true;
      }
      try {
        index(crawlDb, linkDb, segments, noCommit, deleteGone, params, filter, normalize, addBinaryContent, base64);
        return 0;
      } catch (final Exception e) {
        LOG.error("Indexer: {}", StringUtils.stringifyException(e));
        return -1;
      }
    } finally {
      //do nothing
    }
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new IndexingJob(), args);
    System.exit(res);
  }


  //Used for REST API
  @SuppressWarnings("unchecked")
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

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
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.*;

// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.FSUtils;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TimingUtil;

/**
 * This class takes the output of the fetcher and updates the crawldb
 * accordingly.
 */
public class CrawlDb extends NutchTool implements Tool {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static final String CRAWLDB_ADDITIONS_ALLOWED = "db.update.additions.allowed";

  public static final String CRAWLDB_PURGE_404 = "db.update.purge.404";
  public static final String CRAWLDB_PURGE_ORPHANS = "db.update.purge.orphans";

  public static final String CURRENT_NAME = "current";

  public static final String LOCK_NAME = ".locked";

  public CrawlDb() {
  }

  public CrawlDb(Configuration conf) {
    setConf(conf);
  }

  public void update(Path crawlDb, Path[] segments, boolean normalize,
      boolean filter) throws IOException {
    boolean additionsAllowed = getConf().getBoolean(CRAWLDB_ADDITIONS_ALLOWED,
        true);
    update(crawlDb, segments, normalize, filter, additionsAllowed, false);
  }

  public void update(Path crawlDb, Path[] segments, boolean normalize,
      boolean filter, boolean additionsAllowed, boolean force)
      throws IOException {
    Path lock = lock(getConf(), crawlDb, force);

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();

    JobConf job = CrawlDb.createJob(getConf(), crawlDb);
    job.setBoolean(CRAWLDB_ADDITIONS_ALLOWED, additionsAllowed);
    job.setBoolean(CrawlDbFilter.URL_FILTERING, filter);
    job.setBoolean(CrawlDbFilter.URL_NORMALIZING, normalize);

    boolean url404Purging = job.getBoolean(CRAWLDB_PURGE_404, false);

    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb update: starting at " + sdf.format(start));
      LOG.info("CrawlDb update: db: " + crawlDb);
      LOG.info("CrawlDb update: segments: " + Arrays.asList(segments));
      LOG.info("CrawlDb update: additions allowed: " + additionsAllowed);
      LOG.info("CrawlDb update: URL normalizing: " + normalize);
      LOG.info("CrawlDb update: URL filtering: " + filter);
      LOG.info("CrawlDb update: 404 purging: " + url404Purging);
    }

    for (int i = 0; i < segments.length; i++) {
      FileSystem sfs = segments[i].getFileSystem(getConf());
      Path fetch = new Path(segments[i], CrawlDatum.FETCH_DIR_NAME);
      Path parse = new Path(segments[i], CrawlDatum.PARSE_DIR_NAME);
      if (sfs.exists(fetch)) {
        FileInputFormat.addInputPath(job, fetch);
        if (sfs.exists(parse)) {
          FileInputFormat.addInputPath(job, parse);
        } else {
          LOG.info(" - adding fetched but unparsed segment " + segments[i]);
        }
      } else {
        LOG.info(" - skipping invalid segment " + segments[i]);
      }
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb update: Merging segment data into db.");
    }
    try {
      JobClient.runJob(job);
    } catch (IOException e) {
      FileSystem fs = crawlDb.getFileSystem(getConf());
      LockUtil.removeLockFile(fs, lock);
      Path outPath = FileOutputFormat.getOutputPath(job);
      if (fs.exists(outPath))
        fs.delete(outPath, true);
      throw e;
    }

    CrawlDb.install(job, crawlDb);
    long end = System.currentTimeMillis();
    LOG.info("CrawlDb update: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  /*
   * Configure a new CrawlDb in a temp folder at crawlDb/<rand>
   */
  public static JobConf createJob(Configuration config, Path crawlDb)
      throws IOException {
    Path newCrawlDb = new Path(crawlDb, Integer.toString(new Random()
        .nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.setJobName("crawldb " + crawlDb);

    Path current = new Path(crawlDb, CURRENT_NAME);
    if (current.getFileSystem(job).exists(current)) {
      FileInputFormat.addInputPath(job, current);
    }
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(CrawlDbFilter.class);
    job.setReducerClass(CrawlDbReducer.class);

    FileOutputFormat.setOutputPath(job, newCrawlDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    // https://issues.apache.org/jira/browse/NUTCH-1110
    job.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

    return job;
  }

  public static Path lock(Configuration job, Path crawlDb, boolean force) throws IOException {
    Path lock = new Path(crawlDb, LOCK_NAME);
    LockUtil.createLockFile(job, lock, force);
    return lock;
  }

  private static void install(Configuration conf, Path crawlDb, Path tempCrawlDb)
      throws IOException {
    boolean preserveBackup = conf.getBoolean("db.preserve.backup", true);
    FileSystem fs = crawlDb.getFileSystem(conf);
    Path old = new Path(crawlDb, "old");
    Path current = new Path(crawlDb, CURRENT_NAME);
    if (fs.exists(current)) {
      FSUtils.replace(fs, old, current, true);
    }
    FSUtils.replace(fs, current, tempCrawlDb, true);
    Path lock = new Path(crawlDb, LOCK_NAME);
    LockUtil.removeLockFile(fs, lock);
    if (!preserveBackup && fs.exists(old)) {
      fs.delete(old, true);
    }
  }

  // old MapReduce API
  public static void install(JobConf job, Path crawlDb) throws IOException {
    Path tempCrawlDb = FileOutputFormat.getOutputPath(job);
    install(job, crawlDb, tempCrawlDb);
  }

  // new MapReduce API
  public static void install(Job job, Path crawlDb) throws IOException {
    Configuration conf = job.getConfiguration();
    Path tempCrawlDb = org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
        .getOutputPath(job);
    install(conf, crawlDb, tempCrawlDb);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new CrawlDb(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err
          .println("Usage: CrawlDb <crawldb> (-dir <segments> | <seg1> <seg2> ...) [-force] [-normalize] [-filter] [-noAdditions]");
      System.err.println("\tcrawldb\tCrawlDb to update");
      System.err
          .println("\t-dir segments\tparent directory containing all segments to update from");
      System.err
          .println("\tseg1 seg2 ...\tlist of segment names to update from");
      System.err
          .println("\t-force\tforce update even if CrawlDb appears to be locked (CAUTION advised)");
      System.err
          .println("\t-normalize\tuse URLNormalizer on urls in CrawlDb and segment (usually not needed)");
      System.err
          .println("\t-filter\tuse URLFilters on urls in CrawlDb and segment");
      System.err
          .println("\t-noAdditions\tonly update already existing URLs, don't add any newly discovered URLs");

      return -1;
    }
    boolean normalize = getConf().getBoolean(CrawlDbFilter.URL_NORMALIZING,
        false);
    boolean filter = getConf().getBoolean(CrawlDbFilter.URL_FILTERING, false);
    boolean additionsAllowed = getConf().getBoolean(CRAWLDB_ADDITIONS_ALLOWED,
        true);
    boolean force = false;
    HashSet<Path> dirs = new HashSet<>();
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-normalize")) {
        normalize = true;
      } else if (args[i].equals("-filter")) {
        filter = true;
      } else if (args[i].equals("-force")) {
        force = true;
      } else if (args[i].equals("-noAdditions")) {
        additionsAllowed = false;
      } else if (args[i].equals("-dir")) {
        Path dirPath = new Path(args[++i]);
        FileSystem fs = dirPath.getFileSystem(getConf());
        FileStatus[] paths = fs.listStatus(dirPath,
            HadoopFSUtil.getPassDirectoriesFilter(fs));
        dirs.addAll(Arrays.asList(HadoopFSUtil.getPaths(paths)));
      } else {
        dirs.add(new Path(args[i]));
      }
    }
    try {
      update(new Path(args[0]), dirs.toArray(new Path[dirs.size()]), normalize,
          filter, additionsAllowed, force);
      return 0;
    } catch (Exception e) {
      LOG.error("CrawlDb update: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  /*
   * Used for Nutch REST service
   */
  @Override
  public Map<String, Object> run(Map<String, Object> args, String crawlId) throws Exception {

    Map<String, Object> results = new HashMap<>();

    boolean normalize = getConf().getBoolean(CrawlDbFilter.URL_NORMALIZING,
        false);
    boolean filter = getConf().getBoolean(CrawlDbFilter.URL_FILTERING, false);
    boolean additionsAllowed = getConf().getBoolean(CRAWLDB_ADDITIONS_ALLOWED,
        true);
    boolean force = false;
    HashSet<Path> dirs = new HashSet<>();

    if (args.containsKey("normalize")) {
      normalize = true;
    } 
    if (args.containsKey("filter")) {
      filter = true;
    } 
    if (args.containsKey("force")) {
      force = true;
    } 
    if (args.containsKey("noAdditions")) {
      additionsAllowed = false;
    }

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
      dirs.addAll(Arrays.asList(HadoopFSUtil.getPaths(paths)));
    }

    else if(args.containsKey(Nutch.ARG_SEGMENT)) {
      Object segments = args.get(Nutch.ARG_SEGMENT);
      ArrayList<String> segmentList = new ArrayList<>();
      if(segments instanceof ArrayList) {
        segmentList = (ArrayList<String>)segments;
      }
      for(String segment: segmentList) {
        dirs.add(new Path(segment));
      }
    }
    else {
      String segment_dir = crawlId+"/segments";
      File dir = new File(segment_dir);
      File[] segmentsList = dir.listFiles();  
      Arrays.sort(segmentsList, (f1, f2) -> {
        if(f1.lastModified()>f2.lastModified())
          return -1;
        else
          return 0;
      });
      dirs.add(new Path(segmentsList[0].getPath()));
    }
    try {
      update(crawlDb, dirs.toArray(new Path[dirs.size()]), normalize,
          filter, additionsAllowed, force);
      results.put(Nutch.VAL_RESULT, Integer.toString(0));
      return results;
    } catch (Exception e) {
      LOG.error("CrawlDb update: " + StringUtils.stringifyException(e));
      results.put(Nutch.VAL_RESULT, Integer.toString(-1));
      return results;
    }
  }
}

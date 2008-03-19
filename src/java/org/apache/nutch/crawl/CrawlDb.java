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

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/**
 * This class takes the output of the fetcher and updates the
 * crawldb accordingly.
 */
public class CrawlDb extends Configured implements Tool {
  public static final Log LOG = LogFactory.getLog(CrawlDb.class);

  public static final String CRAWLDB_ADDITIONS_ALLOWED = "db.update.additions.allowed";

  public static final String CURRENT_NAME = "current";
  
  public static final String LOCK_NAME = ".locked";
  
  public CrawlDb() {}
  
  public CrawlDb(Configuration conf) {
    setConf(conf);
  }

  public void update(Path crawlDb, Path[] segments, boolean normalize, boolean filter) throws IOException {
    boolean additionsAllowed = getConf().getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);    
    update(crawlDb, segments, normalize, filter, additionsAllowed, false);
  }
  
  public void update(Path crawlDb, Path[] segments, boolean normalize, boolean filter, boolean additionsAllowed, boolean force) throws IOException {
    FileSystem fs = FileSystem.get(getConf());
    Path lock = new Path(crawlDb, LOCK_NAME);
    LockUtil.createLockFile(fs, lock, force);
    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb update: starting");
      LOG.info("CrawlDb update: db: " + crawlDb);
      LOG.info("CrawlDb update: segments: " + Arrays.asList(segments));
      LOG.info("CrawlDb update: additions allowed: " + additionsAllowed);
      LOG.info("CrawlDb update: URL normalizing: " + normalize);
      LOG.info("CrawlDb update: URL filtering: " + filter);
    }

    JobConf job = CrawlDb.createJob(getConf(), crawlDb);
    job.setBoolean(CRAWLDB_ADDITIONS_ALLOWED, additionsAllowed);
    job.setBoolean(CrawlDbFilter.URL_FILTERING, filter);
    job.setBoolean(CrawlDbFilter.URL_NORMALIZING, normalize);
    for (int i = 0; i < segments.length; i++) {
      Path fetch = new Path(segments[i], CrawlDatum.FETCH_DIR_NAME);
      Path parse = new Path(segments[i], CrawlDatum.PARSE_DIR_NAME);
      if (fs.exists(fetch) && fs.exists(parse)) {
        job.addInputPath(fetch);
        job.addInputPath(parse);
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
      LockUtil.removeLockFile(fs, lock);
      if (fs.exists(job.getOutputPath())) fs.delete(job.getOutputPath());
      throw e;
    }

    CrawlDb.install(job, crawlDb);
    if (LOG.isInfoEnabled()) { LOG.info("CrawlDb update: done"); }
  }

  public static JobConf createJob(Configuration config, Path crawlDb)
    throws IOException {
    Path newCrawlDb =
      new Path(crawlDb,
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.setJobName("crawldb " + crawlDb);


    Path current = new Path(crawlDb, CURRENT_NAME);
    if (FileSystem.get(job).exists(current)) {
      job.addInputPath(current);
    }
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(CrawlDbFilter.class);
    job.setReducerClass(CrawlDbReducer.class);

    job.setOutputPath(newCrawlDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    return job;
  }

  public static void install(JobConf job, Path crawlDb) throws IOException {
    Path newCrawlDb = job.getOutputPath();
    FileSystem fs = new JobClient(job).getFs();
    Path old = new Path(crawlDb, "old");
    Path current = new Path(crawlDb, CURRENT_NAME);
    if (fs.exists(current)) {
      if (fs.exists(old)) fs.delete(old);
      fs.rename(current, old);
    }
    fs.mkdirs(crawlDb);
    fs.rename(newCrawlDb, current);
    if (fs.exists(old)) fs.delete(old);
    Path lock = new Path(crawlDb, LOCK_NAME);
    LockUtil.removeLockFile(fs, lock);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new CrawlDb(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: CrawlDb <crawldb> (-dir <segments> | <seg1> <seg2> ...) [-force] [-normalize] [-filter] [-noAdditions]");
      System.err.println("\tcrawldb\tCrawlDb to update");
      System.err.println("\t-dir segments\tparent directory containing all segments to update from");
      System.err.println("\tseg1 seg2 ...\tlist of segment names to update from");
      System.err.println("\t-force\tforce update even if CrawlDb appears to be locked (CAUTION advised)");
      System.err.println("\t-normalize\tuse URLNormalizer on urls in CrawlDb and segment (usually not needed)");
      System.err.println("\t-filter\tuse URLFilters on urls in CrawlDb and segment");
      System.err.println("\t-noAdditions\tonly update already existing URLs, don't add any newly discovered URLs");
      return -1;
    }
    boolean normalize = false;
    boolean filter = false;
    boolean force = false;
    final FileSystem fs = FileSystem.get(getConf());
    boolean additionsAllowed = getConf().getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);
    HashSet<Path> dirs = new HashSet<Path>();
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
        FileStatus[] paths = fs.listStatus(new Path(args[++i]), HadoopFSUtil.getPassDirectoriesFilter(fs));
        dirs.addAll(Arrays.asList(HadoopFSUtil.getPaths(paths)));
      } else {
        dirs.add(new Path(args[i]));
      }
    }
    try {
      update(new Path(args[0]), dirs.toArray(new Path[dirs.size()]), normalize, filter, additionsAllowed, force);
      return 0;
    } catch (Exception e) {
      LOG.fatal("CrawlDb update: " + StringUtils.stringifyException(e));
      return -1;
    }
  }
}

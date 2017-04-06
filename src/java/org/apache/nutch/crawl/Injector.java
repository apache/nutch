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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.service.NutchServer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TimingUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Injector takes a flat file of URLs and merges ("injects") these URLs into the
 * CrawlDb. Useful for bootstrapping a Nutch crawl. The URL files contain one
 * URL per line, optionally followed by custom metadata separated by tabs with
 * the metadata key separated from the corresponding value by '='.
 * </p>
 * <p>
 * Note, that some metadata keys are reserved:
 * <dl>
 * <dt>nutch.score</dt>
 * <dd>allows to set a custom score for a specific URL</dd>
 * <dt>nutch.fetchInterval</dt>
 * <dd>allows to set a custom fetch interval for a specific URL</dd>
 * <dt>nutch.fetchInterval.fixed</dt>
 * <dd>allows to set a custom fetch interval for a specific URL that is not
 * changed by AdaptiveFetchSchedule</dd>
 * </dl>
 * </p>
 * <p>
 * Example:
 * 
 * <pre>
 *  http://www.nutch.org/ \t nutch.score=10 \t nutch.fetchInterval=2592000 \t userType=open_source
 * </pre>
 * </p>
 **/
public class Injector extends NutchTool implements Tool {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /** property to pass value of command-line option -filterNormalizeAll to mapper */
  public static final String URL_FILTER_NORMALIZE_ALL = "crawldb.inject.filter.normalize.all";

  /** metadata key reserved for setting a custom score for a specific URL */
  public static String nutchScoreMDName = "nutch.score";

  /**
   * metadata key reserved for setting a custom fetchInterval for a specific URL
   */
  public static String nutchFetchIntervalMDName = "nutch.fetchInterval";

  /**
   * metadata key reserved for setting a fixed custom fetchInterval for a
   * specific URL
   */
  public static String nutchFixedFetchIntervalMDName = "nutch.fetchInterval.fixed";

  public static class InjectMapper
      extends Mapper<Text, Writable, Text, CrawlDatum> {
    public static final String URL_NORMALIZING_SCOPE = "crawldb.url.normalizers.scope";
    public static final String TAB_CHARACTER = "\t";
    public static final String EQUAL_CHARACTER = "=";

    private URLNormalizers urlNormalizers;
    private int interval;
    private float scoreInjected;
    private URLFilters filters;
    private ScoringFilters scfilters;
    private long curTime;
    private boolean url404Purging;
    private String scope;
    private boolean filterNormalizeAll = false;

    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      boolean normalize = conf.getBoolean(CrawlDbFilter.URL_NORMALIZING, true);
      boolean filter = conf.getBoolean(CrawlDbFilter.URL_FILTERING, true);
      filterNormalizeAll = conf.getBoolean(URL_FILTER_NORMALIZE_ALL, false);
      if (normalize) {
        scope = conf.get(URL_NORMALIZING_SCOPE, URLNormalizers.SCOPE_INJECT);
        urlNormalizers = new URLNormalizers(conf, scope);
      }
      interval = conf.getInt("db.fetch.interval.default", 2592000);
      if (filter) {
        filters = new URLFilters(conf);
      }
      scfilters = new ScoringFilters(conf);
      scoreInjected = conf.getFloat("db.score.injected", 1.0f);
      curTime = conf.getLong("injector.current.time",
          System.currentTimeMillis());
      url404Purging = conf.getBoolean(CrawlDb.CRAWLDB_PURGE_404, false);
    }

    /* Filter and normalize the input url */
    private String filterNormalize(String url) {
      if (url != null) {
        try {
          if (urlNormalizers != null)
            url = urlNormalizers.normalize(url, scope); // normalize the url
          if (filters != null)
            url = filters.filter(url); // filter the url
        } catch (Exception e) {
          LOG.warn("Skipping " + url + ":" + e);
          url = null;
        }
      }
      return url;
    }

    /**
     * Extract metadata that could be passed along with url in a seeds file.
     * Metadata must be key-value pair(s) and separated by a TAB_CHARACTER
     */
    private void processMetaData(String metadata, CrawlDatum datum,
        String url) {
      String[] splits = metadata.split(TAB_CHARACTER);

      for (String split : splits) {
        // find separation between name and value
        int indexEquals = split.indexOf(EQUAL_CHARACTER);
        if (indexEquals == -1) // skip anything without a EQUAL_CHARACTER
          continue;

        String metaname = split.substring(0, indexEquals);
        String metavalue = split.substring(indexEquals + 1);

        try {
          if (metaname.equals(nutchScoreMDName)) {
            datum.setScore(Float.parseFloat(metavalue));
          } else if (metaname.equals(nutchFetchIntervalMDName)) {
            datum.setFetchInterval(Integer.parseInt(metavalue));
          } else if (metaname.equals(nutchFixedFetchIntervalMDName)) {
            int fixedInterval = Integer.parseInt(metavalue);
            if (fixedInterval > -1) {
              // Set writable using float. Float is used by
              // AdaptiveFetchSchedule
              datum.getMetaData().put(Nutch.WRITABLE_FIXED_INTERVAL_KEY,
                  new FloatWritable(fixedInterval));
              datum.setFetchInterval(fixedInterval);
            }
          } else {
            datum.getMetaData().put(new Text(metaname), new Text(metavalue));
          }
        } catch (NumberFormatException nfe) {
          LOG.error("Invalid number '" + metavalue + "' in metadata '"
              + metaname + "' for url " + url);
        }
      }
    }

    public void map(Text key, Writable value, Context context)
        throws IOException, InterruptedException {
      if (value instanceof Text) {
        // if its a url from the seed list
        String url = key.toString().trim();

        // remove empty string or string starting with '#'
        if (url.length() == 0 || url.startsWith("#"))
          return;

        url = filterNormalize(url);
        if (url == null) {
          context.getCounter("injector", "urls_filtered").increment(1);
        } else {
          CrawlDatum datum = new CrawlDatum();
          datum.setStatus(CrawlDatum.STATUS_INJECTED);
          datum.setFetchTime(curTime);
          datum.setScore(scoreInjected);
          datum.setFetchInterval(interval);

          String metadata = value.toString().trim();
          if (metadata.length() > 0)
            processMetaData(metadata, datum, url);

          try {
            key.set(url);
            scfilters.injectedScore(key, datum);
          } catch (ScoringFilterException e) {
            if (LOG.isWarnEnabled()) {
              LOG.warn("Cannot filter injected score for url " + url
                  + ", using default (" + e.getMessage() + ")");
            }
          }
          context.getCounter("injector", "urls_injected").increment(1);
          context.write(key, datum);
        }
      } else if (value instanceof CrawlDatum) {
        // if its a crawlDatum from the input crawldb, emulate CrawlDbFilter's
        // map()
        CrawlDatum datum = (CrawlDatum) value;

        // remove 404 urls
        if (url404Purging && CrawlDatum.STATUS_DB_GONE == datum.getStatus())
          return;

        if (filterNormalizeAll) {
          String url = filterNormalize(key.toString());
          if (url != null) {
            key.set(url);
            context.write(key, datum);
          }
        } else {
          context.write(key, datum);
        }
      }
    }
  }

  /** Combine multiple new entries for a url. */
  public static class InjectReducer
      extends Reducer<Text, CrawlDatum, Text, CrawlDatum> {
    private int interval;
    private float scoreInjected;
    private boolean overwrite = false;
    private boolean update = false;
    private CrawlDatum old = new CrawlDatum();
    private CrawlDatum injected = new CrawlDatum();

    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      interval = conf.getInt("db.fetch.interval.default", 2592000);
      scoreInjected = conf.getFloat("db.score.injected", 1.0f);
      overwrite = conf.getBoolean("db.injector.overwrite", false);
      update = conf.getBoolean("db.injector.update", false);
      LOG.info("Injector: overwrite: " + overwrite);
      LOG.info("Injector: update: " + update);
    }

    /**
     * Merge the input records as per rules below :
     * 
     * <pre>
     * 1. If there is ONLY new injected record ==> emit injected record
     * 2. If there is ONLY old record          ==> emit existing record
     * 3. If BOTH new and old records are present:
     *    (a) If 'overwrite' is true           ==> emit injected record
     *    (b) If 'overwrite' is false :
     *        (i)  If 'update' is false        ==> emit existing record
     *        (ii) If 'update' is true         ==> update existing record and emit it
     * </pre>
     * 
     * For more details @see NUTCH-1405
     */
    public void reduce(Text key, Iterable<CrawlDatum> values, Context context)
        throws IOException, InterruptedException {

      boolean oldSet = false;
      boolean injectedSet = false;

      // If we encounter a datum with status as STATUS_INJECTED, then its a
      // newly injected record. All other statuses correspond to an old record.
      for (CrawlDatum val : values) {
        if (val.getStatus() == CrawlDatum.STATUS_INJECTED) {
          injected.set(val);
          injected.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
          injectedSet = true;
        } else {
          old.set(val);
          oldSet = true;
        }
      }

      CrawlDatum result;
      if (injectedSet && (!oldSet || overwrite)) {
        // corresponds to rules (1) and (3.a) in the method description
        result = injected;
      } else {
        // corresponds to rules (2) and (3.b) in the method description
        result = old;

        if (injectedSet && update) {
          // corresponds to rule (3.b.ii) in the method description
          old.putAllMetaData(injected);
          old.setScore(injected.getScore() != scoreInjected
              ? injected.getScore() : old.getScore());
          old.setFetchInterval(injected.getFetchInterval() != interval
              ? injected.getFetchInterval() : old.getFetchInterval());
        }
      }
      if (injectedSet && oldSet) {
        context.getCounter("injector", "urls_merged").increment(1);
      }
      context.write(key, result);
    }
  }

  public Injector() {
  }

  public Injector(Configuration conf) {
    setConf(conf);
  }

  public void inject(Path crawlDb, Path urlDir)
      throws IOException, ClassNotFoundException, InterruptedException {
    inject(crawlDb, urlDir, false, false);
  }

  public void inject(Path crawlDb, Path urlDir, boolean overwrite,
      boolean update) throws IOException, ClassNotFoundException, InterruptedException {
    inject(crawlDb, urlDir, overwrite, update, true, true, false);
  }

  public void inject(Path crawlDb, Path urlDir, boolean overwrite,
      boolean update, boolean normalize, boolean filter,
      boolean filterNormalizeAll)
      throws IOException, ClassNotFoundException, InterruptedException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();

    if (LOG.isInfoEnabled()) {
      LOG.info("Injector: starting at " + sdf.format(start));
      LOG.info("Injector: crawlDb: " + crawlDb);
      LOG.info("Injector: urlDir: " + urlDir);
      LOG.info("Injector: Converting injected urls to crawl db entries.");
    }

    // set configuration
    Configuration conf = getConf();
    conf.setLong("injector.current.time", System.currentTimeMillis());
    conf.setBoolean("db.injector.overwrite", overwrite);
    conf.setBoolean("db.injector.update", update);
    conf.setBoolean(CrawlDbFilter.URL_NORMALIZING, normalize);
    conf.setBoolean(CrawlDbFilter.URL_FILTERING, filter);
    conf.setBoolean(URL_FILTER_NORMALIZE_ALL, filterNormalizeAll);
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

    // create all the required paths
    FileSystem fs = crawlDb.getFileSystem(conf);
    Path current = new Path(crawlDb, CrawlDb.CURRENT_NAME);
    if (!fs.exists(current))
      fs.mkdirs(current);

    Path tempCrawlDb = new Path(crawlDb,
        "crawldb-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    // lock an existing crawldb to prevent multiple simultaneous updates
    Path lock = CrawlDb.lock(conf, crawlDb, false);

    // configure job
    Job job = Job.getInstance(conf, "inject " + urlDir);
    job.setJarByClass(Injector.class);
    job.setMapperClass(InjectMapper.class);
    job.setReducerClass(InjectReducer.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);
    job.setSpeculativeExecution(false);

    // set input and output paths of the job
    MultipleInputs.addInputPath(job, current, SequenceFileInputFormat.class);
    MultipleInputs.addInputPath(job, urlDir, KeyValueTextInputFormat.class);
    FileOutputFormat.setOutputPath(job, tempCrawlDb);

    try {
      // run the job
      job.waitForCompletion(true);

      // save output and perform cleanup
      CrawlDb.install(job, crawlDb);

      if (LOG.isInfoEnabled()) {
        long urlsInjected = job.getCounters()
            .findCounter("injector", "urls_injected").getValue();
        long urlsFiltered = job.getCounters()
            .findCounter("injector", "urls_filtered").getValue();
        long urlsMerged = job.getCounters()
            .findCounter("injector", "urls_merged").getValue();
        LOG.info("Injector: Total urls rejected by filters: " + urlsFiltered);
        LOG.info(
            "Injector: Total urls injected after normalization and filtering: "
                + urlsInjected);
        LOG.info("Injector: Total urls injected but already in CrawlDb: "
            + urlsMerged);
        LOG.info("Injector: Total new urls injected: "
            + (urlsInjected - urlsMerged));

        long end = System.currentTimeMillis();
        LOG.info("Injector: finished at " + sdf.format(end) + ", elapsed: "
            + TimingUtil.elapsedTime(start, end));
      }
    } catch (IOException e) {
      if (fs.exists(tempCrawlDb)) {
        fs.delete(tempCrawlDb, true);
      }
      LockUtil.removeLockFile(conf, lock);
      throw e;
    }
  }

  public void usage() {
    System.err.println(
        "Usage: Injector <crawldb> <url_dir> [-overwrite|-update] [-noFilter] [-noNormalize] [-filterNormalizeAll]\n");
    System.err.println(
        "  <crawldb>\tPath to a crawldb directory. If not present, a new one would be created.");
    System.err.println(
        "  <url_dir>\tPath to directory with URL file(s) containing urls to be injected. A URL file");
    System.err.println(
        "           \tshould have one URL per line, optionally followed by custom metadata.");
    System.err.println(
        "           \tBlank lines or lines starting with a '#' would be ignored. Custom metadata must");
    System.err
        .println("           \tbe of form 'key=value' and separated by tabs.");
    System.err.println("           \tBelow are reserved metadata keys:\n");
    System.err.println("           \t\tnutch.score: A custom score for a url");
    System.err.println(
        "           \t\tnutch.fetchInterval: A custom fetch interval for a url");
    System.err.println(
        "           \t\tnutch.fetchInterval.fixed: A custom fetch interval for a url that is not "
            + "changed by AdaptiveFetchSchedule\n");
    System.err.println("           \tExample:");
    System.err.println("           \t http://www.apache.org/");
    System.err.println(
        "           \t http://www.nutch.org/ \\t nutch.score=10 \\t nutch.fetchInterval=2592000 \\t userType=open_source\n");
    System.err.println(
        " -overwrite\tOverwite existing crawldb records by the injected records. Has precedence over 'update'");
    System.err.println(
        " -update   \tUpdate existing crawldb records with the injected records. Old metadata is preserved");
    System.err.println();
    System.err.println(
        " -nonormalize\tDo not normalize URLs before injecting");
    System.err.println(
        " -nofilter \tDo not apply URL filters to injected URLs");
    System.err.println(
        " -filterNormalizeAll\tNormalize and filter all URLs including the URLs of existing CrawlDb records");
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new Injector(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      usage();
      return -1;
    }

    boolean overwrite = false;
    boolean update = false;
    boolean normalize = true;
    boolean filter = true;
    boolean filterNormalizeAll = false;

    for (int i = 2; i < args.length; i++) {
      if (args[i].equals("-overwrite")) {
        overwrite = true;
      } else if (args[i].equals("-update")) {
        update = true;
      } else if (args[i].equals("-noNormalize")) {
        normalize = false;
      } else if (args[i].equals("-noFilter")) {
        filter = false;
      } else if (args[i].equals("-filterNormalizeAll")) {
        filterNormalizeAll = true;
      } else {
        LOG.info("Injector: Found invalid argument \"" + args[i] + "\"\n");
        usage();
        return -1;
      }
    }

    try {
      inject(new Path(args[0]), new Path(args[1]), overwrite, update, normalize,
          filter, filterNormalizeAll);
      return 0;
    } catch (Exception e) {
      LOG.error("Injector: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  /**
   * Used by the Nutch REST service
   */
  public Map<String, Object> run(Map<String, Object> args, String crawlId)
      throws Exception {
    if(args.size()<1){
      throw new IllegalArgumentException("Required arguments <url_dir> or <seedName>");
    }
    Path input;
    Object path = null;
    if(args.containsKey(Nutch.ARG_SEEDDIR)) {
      path = args.get(Nutch.ARG_SEEDDIR);
    }
    else if(args.containsKey(Nutch.ARG_SEEDNAME)) {
      path = NutchServer.getInstance().getSeedManager().
          getSeedList((String)args.get(Nutch.ARG_SEEDNAME)).getSeedFilePath();
    }
    else {
      throw new IllegalArgumentException("Required arguments <url_dir> or <seedName>");
    }
    if(path instanceof Path) {
      input = (Path) path;
    }
    else {
      input = new Path(path.toString());
    }
    Map<String, Object> results = new HashMap<>();
    Path crawlDb;
    if (args.containsKey(Nutch.ARG_CRAWLDB)) {
      Object crawldbPath = args.get(Nutch.ARG_CRAWLDB);
      if (crawldbPath instanceof Path) {
        crawlDb = (Path) crawldbPath;
      } else {
        crawlDb = new Path(crawldbPath.toString());
      }
    } else {
      crawlDb = new Path(crawlId + "/crawldb");
    }
    inject(crawlDb, input);
    results.put(Nutch.VAL_RESULT, Integer.toString(0));
    return results;
  }

}

/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.fetcher;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import org.apache.avro.util.Utf8;
import org.apache.gora.filter.FilterOp;
import org.apache.gora.filter.MapFieldValueFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.URLPartitioner.FetchEntryPartitioner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.ToolUtil;
import org.apache.gora.mapreduce.GoraMapper;

/**
 * Multi-threaded fetcher.
 * 
 */
public class FetcherJob extends NutchTool implements Tool {

  public static final String PROTOCOL_REDIR = "protocol";

  public static final int PERM_REFRESH_TIME = 5;

  public static final Utf8 REDIRECT_DISCOVERED = new Utf8("___rdrdsc__");

  public static final String RESUME_KEY = "fetcher.job.resume";
  public static final String SITEMAP = "fetcher.job.sitemap";
  public static final String SITEMAP_DETECT = "fetcher.job.sitemap.detect";
  public static final String PARSE_KEY = "fetcher.parse";
  public static final String THREADS_KEY = "fetcher.threads.fetch";

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.REPR_URL);
    FIELDS.add(WebPage.Field.FETCH_TIME);
  }

  /**
   * <p>
   * Mapper class for Fetcher.
   * </p>
   * <p>
   * This class reads the random integer written by {@link GeneratorJob} as its
   * key while outputting the actual key and value arguments through a
   * {@link FetchEntry} instance.
   * </p>
   * <p>
   * This approach (combined with the use of {@link PartitionUrlByHost}) makes
   * sure that Fetcher is still polite while also randomizing the key order. If
   * one host has a huge number of URLs in your table while other hosts have
   * not, {@link FetcherReducer} will not be stuck on one host but process URLs
   * from other hosts as well.
   * </p>
   */
  public static class FetcherMapper extends
  GoraMapper<String, WebPage, IntWritable, FetchEntry> {

    private boolean shouldContinue;

    private Utf8 batchId;

    private Random random = new Random();

    @Override
    protected void setup(Context context) {
      Configuration conf = context.getConfiguration();
      shouldContinue = conf.getBoolean(RESUME_KEY, false);
      batchId = new Utf8(
          conf.get(GeneratorJob.BATCH_ID, Nutch.ALL_BATCH_ID_STR));
    }

    @Override
    protected void map(String key, WebPage page, Context context)
        throws IOException, InterruptedException {
      if (Mark.GENERATE_MARK.checkMark(page) == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping " + TableUtil.unreverseUrl(key)
          + "; not generated yet");
        }
        return;
      }
      if (shouldContinue && Mark.FETCH_MARK.checkMark(page) != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping " + TableUtil.unreverseUrl(key)
          + "; already fetched");
        }
        return;
      }
      boolean sitemap = context.getConfiguration().getBoolean(SITEMAP, false);

      if ((sitemap && !URLFilters.isSitemap(page)) || !sitemap && URLFilters
          .isSitemap(page))
        return;
      context.write(new IntWritable(random.nextInt(65536)), new FetchEntry(
          context.getConfiguration(), key, page));
    }
  }

  public static final Logger LOG = LoggerFactory.getLogger(FetcherJob.class);

  public FetcherJob() {

  }

  public FetcherJob(Configuration conf) {
    setConf(conf);
  }

  public Collection<WebPage.Field> getFields(Job job) {
    Collection<WebPage.Field> fields = new HashSet<WebPage.Field>(FIELDS);
    if (job.getConfiguration().getBoolean(PARSE_KEY, false)) {
      ParserJob parserJob = new ParserJob();
      fields.addAll(parserJob.getFields(job));
    }
    ProtocolFactory protocolFactory = new ProtocolFactory(
        job.getConfiguration());
    fields.addAll(protocolFactory.getFields());

    return fields;
  }

  @Override
  public Map<String, Object> run(Map<String, Object> args) throws Exception {
    checkConfiguration();
    String batchId = (String) args.get(Nutch.ARG_BATCH);
    Integer threads = (Integer) args.get(Nutch.ARG_THREADS);
    Boolean shouldResume = (Boolean) args.get(Nutch.ARG_RESUME);
    Integer numTasks = (Integer) args.get(Nutch.ARG_NUMTASKS);
    Boolean stmDetect = (Boolean) args.get(Nutch.ARG_SITEMAP_DETECT);
    Boolean sitemap = (Boolean) args.get(Nutch.ARG_SITEMAP);

    if (threads != null && threads > 0) {
      getConf().setInt(THREADS_KEY, threads);
    }
    if (batchId == null) {
      batchId = Nutch.ALL_BATCH_ID_STR;
    }
    getConf().set(GeneratorJob.BATCH_ID, batchId);
    if (shouldResume != null) {
      getConf().setBoolean(RESUME_KEY, shouldResume);
    }
    if (stmDetect != null) {
      getConf().setBoolean(SITEMAP_DETECT, stmDetect);
    }
    if (sitemap != null) {
      getConf().setBoolean(SITEMAP, sitemap);
    }

    LOG.info("FetcherJob: threads: {}", getConf().getInt(THREADS_KEY, 10));
    LOG.info("FetcherJob: parsing: {}", getConf().getBoolean(PARSE_KEY, false));
    LOG.info("FetcherJob: resuming: {}", getConf().getBoolean(RESUME_KEY, false));

    // set the actual time for the timelimit relative
    // to the beginning of the whole job and not of a specific task
    // otherwise it keeps trying again if a task fails
    long timelimit = getConf().getLong("fetcher.timelimit.mins", -1);
    if (timelimit != -1) {
      timelimit = System.currentTimeMillis() + (timelimit * 60 * 1000);
      getConf().setLong("fetcher.timelimit", timelimit);
    }
    LOG.info("FetcherJob : timelimit set for : {}", getConf().getLong("fetcher.timelimit", -1));
    numJobs = 1;
    currentJob = NutchJob.getInstance(getConf(), "fetch");

    // for politeness, don't permit parallel execution of a single task
    currentJob.setReduceSpeculativeExecution(false);

    Collection<WebPage.Field> fields = getFields(currentJob);
    MapFieldValueFilter<String, WebPage> batchIdFilter = getBatchIdFilter(batchId);
    StorageUtils.initMapperJob(currentJob, fields, IntWritable.class,
        FetchEntry.class, FetcherMapper.class, FetchEntryPartitioner.class,
        batchIdFilter, false);
    StorageUtils.initReducerJob(currentJob, FetcherReducer.class);
    if (numTasks == null || numTasks < 1) {
      currentJob.setNumReduceTasks(currentJob.getConfiguration().getInt(
          "mapred.map.tasks", currentJob.getNumReduceTasks()));
    } else {
      currentJob.setNumReduceTasks(numTasks);
    }
    currentJob.waitForCompletion(true);
    ToolUtil.recordJobStatus(null, currentJob, results);
    return results;
  }

  private MapFieldValueFilter<String, WebPage> getBatchIdFilter(String batchId) {
    if (batchId.equals(Nutch.ALL_CRAWL_ID.toString())) {
      return null;
    }
    MapFieldValueFilter<String, WebPage> filter = new MapFieldValueFilter<String, WebPage>();
    filter.setFieldName(WebPage.Field.MARKERS.toString());
    filter.setFilterOp(FilterOp.EQUALS);
    filter.setFilterIfMissing(true);
    filter.setMapKey(Mark.GENERATE_MARK.getName());
    filter.getOperands().add(new Utf8(batchId));
    return filter;
  }

  /**
   * Run fetcher.
   * 
   * @param batchId
   *          batchId (obtained from Generator) or null to fetch all generated
   *          fetchlists
   * @param threads
   *          number of threads per map task
   * @param shouldResume
   * @param numTasks
   *          number of fetching tasks (reducers). If set to < 1 then use the
   *          default, which is mapred.map.tasks.
   * @return 0 on success
   * @throws Exception
   */
  public int fetch(String batchId, int threads, boolean shouldResume,
      int numTasks) throws Exception {
    return fetch(batchId, threads, shouldResume, numTasks, false, false);
  }

  /**
   * Run fetcher.
   *
   * @param batchId
   *          batchId (obtained from Generator) or null to fetch all generated
   *          fetchlists
   * @param threads
   *          number of threads per map task
   * @param shouldResume
   * @param numTasks
   *          number of fetching tasks (reducers). If set to < 1 then use the
   *          default, which is mapred.map.tasks.
   * @param stmDetect
   *          If set true, sitemap detection is run.
   * @param sitemap
   *          If set true, only sitemap files is fetched, If set false, only
   *          normal urls is fetched.
   * @return 0 on success
   * @throws Exception
   */
  public int fetch(String batchId, int threads, boolean shouldResume,
      int numTasks, boolean stmDetect, boolean sitemap) throws Exception {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("FetcherJob: starting at " + sdf.format(start));

    if (batchId.equals(Nutch.ALL_BATCH_ID_STR)) {
      LOG.info("FetcherJob: fetching all");
    } else {
      LOG.info("FetcherJob: batchId: " + batchId);
    }

    run(ToolUtil.toArgMap(Nutch.ARG_BATCH, batchId, Nutch.ARG_THREADS, threads,
        Nutch.ARG_RESUME, shouldResume, Nutch.ARG_NUMTASKS, numTasks,
        Nutch.ARG_SITEMAP_DETECT, stmDetect, Nutch.ARG_SITEMAP, sitemap));

    long finish = System.currentTimeMillis();
    LOG.info("FetcherJob: finished at " + sdf.format(finish)
    + ", time elapsed: " + TimingUtil.elapsedTime(start, finish));

    return 0;
  }

  void checkConfiguration() {
    // ensure that a value has been set for the agent name
    String agentName = getConf().get("http.agent.name");
    if (agentName == null || agentName.trim().length() == 0) {
      String message = "Fetcher: No agents listed in 'http.agent.name'"
          + " property.";
      if (LOG.isErrorEnabled()) {
        LOG.error(message);
      }
      throw new IllegalArgumentException(message);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    int threads = -1;
    boolean shouldResume = false;
    boolean stmRobot = false, sitemap = false;
    String batchId;

    String usage = "Usage: FetcherJob (<batchId> | -all) [-crawlId <id>] "
        + "[-threads N] \n \t \t  [-resume] [-numTasks N]\n"
        + "    <batchId>     - crawl identifier returned by Generator, or -all for all \n \t \t    generated batchId-s\n"
        + "    -crawlId <id> - the id to prefix the schemas to operate on, \n \t \t    (default: storage.crawl.id)\n"
        + "    -threads N    - number of fetching threads per task\n"
        + "    -resume       - resume interrupted job\n"
        + "    -numTasks N   - if N > 0 then use this many reduce tasks for fetching \n \t \t    (default: mapred.map.tasks)"
        + "    -sitemap      - only sitemap files are fetched, defaults to false"
        + "    -stmDetect    - sitemap files are detected from robot.txt file";

    if (args.length == 0) {
      System.err.println(usage);
      return -1;
    }

    batchId = args[0];
    if (!batchId.equals("-all") && batchId.startsWith("-")) {
      System.err.println(usage);
      return -1;
    }
    int numTasks = -1;
    for (int i = 1; i < args.length; i++) {
      if ("-threads".equals(args[i])) {
        // found -threads option
        threads = Integer.parseInt(args[++i]);
      } else if ("-resume".equals(args[i])) {
        shouldResume = true;
      } else if ("-numTasks".equals(args[i])) {
        numTasks = Integer.parseInt(args[++i]);
      } else if ("-crawlId".equals(args[i])) {
        getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
      } else if ("-sitemap".equals(args[i])) {
        sitemap = true;
      } else if ("-stmDetect".equals(args[i])) {
        stmRobot = true;
      } else {
        throw new IllegalArgumentException("arg " + args[i] + " not recognized");
      }
    }

    int fetchcode = fetch(batchId, threads, shouldResume, numTasks, stmRobot,
        sitemap); // run the Fetcher

    return fetchcode;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new FetcherJob(),
        args);
    System.exit(res);
  }
}

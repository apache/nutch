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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TableUtil;
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
  public static final String PARSE_KEY = "fetcher.parse";
  public static final String THREADS_KEY = "fetcher.threads.fetch";

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.REPR_URL);
  }

  /**
   * <p>
   * Mapper class for Fetcher.
   * </p>
   * <p>
   * This class reads the random integer written by {@link GeneratorJob} as its key
   * while outputting the actual key and value arguments through a
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
  public static class FetcherMapper
  extends GoraMapper<String, WebPage, IntWritable, FetchEntry> {

    private boolean shouldContinue;

    private Utf8 batchId;

    private Random random = new Random();

    @Override
    protected void setup(Context context) {
      Configuration conf = context.getConfiguration();
      shouldContinue = conf.getBoolean(RESUME_KEY, false);
      batchId = new Utf8(conf.get(GeneratorJob.BATCH_ID, Nutch.ALL_BATCH_ID_STR));
    }

    @Override
    protected void map(String key, WebPage page, Context context)
        throws IOException, InterruptedException {
      Utf8 mark = Mark.GENERATE_MARK.checkMark(page);
      if (!NutchJob.shouldProcess(mark, batchId)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping " + TableUtil.unreverseUrl(key) + "; different batch id");
        }
        return;
      }
      if (shouldContinue && Mark.FETCH_MARK.checkMark(page) != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping " + TableUtil.unreverseUrl(key) + "; already fetched");
        }
        return;
      }
      context.write(new IntWritable(random.nextInt(65536)), new FetchEntry(context
          .getConfiguration(), key, page));
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
    if (job.getConfiguration().getBoolean(PARSE_KEY, true)) {
      ParserJob parserJob = new ParserJob();
      fields.addAll(parserJob.getFields(job));
    }
    ProtocolFactory protocolFactory = new ProtocolFactory(job.getConfiguration());
    fields.addAll(protocolFactory.getFields());

    return fields;
  }

  @Override
  public Map<String,Object> run(Map<String,Object> args) throws Exception {
    checkConfiguration();
    String batchId = (String)args.get(Nutch.ARG_BATCH);
    Integer threads = (Integer)args.get(Nutch.ARG_THREADS);
    Boolean shouldResume = (Boolean)args.get(Nutch.ARG_RESUME);
    Boolean parse = (Boolean)args.get(Nutch.ARG_PARSE);
    Integer numTasks = (Integer)args.get(Nutch.ARG_NUMTASKS);
 
    if (threads != null && threads > 0) {
      getConf().setInt(THREADS_KEY, threads);
    }
    if (batchId == null) {
      batchId = Nutch.ALL_BATCH_ID_STR;
    }
    getConf().set(GeneratorJob.BATCH_ID, batchId);
    if (parse != null) {
      getConf().setBoolean(PARSE_KEY, parse);
    }
    if (shouldResume != null) {
      getConf().setBoolean(RESUME_KEY, shouldResume);
    }

    // set the actual time for the timelimit relative
    // to the beginning of the whole job and not of a specific task
    // otherwise it keeps trying again if a task fails
    long timelimit = getConf().getLong("fetcher.timelimit.mins", -1);
    if (timelimit != -1) {
      timelimit = System.currentTimeMillis() + (timelimit * 60 * 1000);
      getConf().setLong("fetcher.timelimit", timelimit);
    }
    numJobs = 1;
    currentJob = new NutchJob(getConf(), "fetch");
    Collection<WebPage.Field> fields = getFields(currentJob);
    StorageUtils.initMapperJob(currentJob, fields, IntWritable.class,
        FetchEntry.class, FetcherMapper.class, PartitionUrlByHost.class, false);
    StorageUtils.initReducerJob(currentJob, FetcherReducer.class);
    if (numTasks == null || numTasks < 1) {
      currentJob.setNumReduceTasks(currentJob.getConfiguration().getInt("mapred.map.tasks",
          currentJob.getNumReduceTasks()));
    } else {
      currentJob.setNumReduceTasks(numTasks);
    }
    currentJob.waitForCompletion(true);
    ToolUtil.recordJobStatus(null, currentJob, results);
    return results;
  }

  /**
   * Run fetcher.
   * @param batchId batchId (obtained from Generator) or null to fetch all generated fetchlists
   * @param threads number of threads per map task
   * @param shouldResume
   * @param parse if true, then parse content immediately, if false then a separate
   * run of {@link ParserJob} will be needed.
   * @param numTasks number of fetching tasks (reducers). If set to < 1 then use the default,
   * which is mapred.map.tasks.
   * @return 0 on success
   * @throws Exception
   */
  public int fetch(String batchId, int threads, boolean shouldResume, boolean parse, int numTasks)
      throws Exception {
    LOG.info("FetcherJob: starting");

    LOG.info("FetcherJob : timelimit set for : " + getConf().getLong("fetcher.timelimit", -1));
    LOG.info("FetcherJob: threads: " + getConf().getInt(THREADS_KEY, 10));
    LOG.info("FetcherJob: parsing: " + getConf().getBoolean(PARSE_KEY, true));
    LOG.info("FetcherJob: resuming: " + getConf().getBoolean(RESUME_KEY, false));
    if (batchId.equals(Nutch.ALL_BATCH_ID_STR)) {
      LOG.info("FetcherJob: fetching all");
    } else {
      LOG.info("FetcherJob: batchId: " + batchId);
    }

    run(ToolUtil.toArgMap(
        Nutch.ARG_BATCH, batchId,
        Nutch.ARG_THREADS, threads,
        Nutch.ARG_RESUME, shouldResume,
        Nutch.ARG_PARSE, parse,
        Nutch.ARG_NUMTASKS, numTasks));
    LOG.info("FetcherJob: done");
    return 0;
  }

  void checkConfiguration() {

    // ensure that a value has been set for the agent name and that that
    // agent name is the first value in the agents we advertise for robot
    // rules parsing
    String agentName = getConf().get("http.agent.name");
    if (agentName == null || agentName.trim().length() == 0) {
      String message = "Fetcher: No agents listed in 'http.agent.name'"
          + " property.";
      if (LOG.isErrorEnabled()) {
        LOG.error(message);
      }
      throw new IllegalArgumentException(message);
    } else {

      // get all of the agents that we advertise
      String agentNames = getConf().get("http.robots.agents");
      StringTokenizer tok = new StringTokenizer(agentNames, ",");
      ArrayList<String> agents = new ArrayList<String>();
      while (tok.hasMoreTokens()) {
        agents.add(tok.nextToken().trim());
      }

      // if the first one is not equal to our agent name, log fatal and throw
      // an exception
      if (!(agents.get(0)).equalsIgnoreCase(agentName)) {
        String message = "Fetcher: Your 'http.agent.name' value should be "
            + "listed first in 'http.robots.agents' property.";
        LOG.warn(message);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    int threads = -1;
    boolean shouldResume = false;
    boolean parse = getConf().getBoolean(PARSE_KEY, false);
    String batchId;

    String usage = "Usage: FetcherJob (<batchId> | -all) [-crawlId <id>] " +
      "[-threads N] [-parse] [-resume] [-numTasks N]\n" +
      "\tbatchId\tcrawl identifier returned by Generator, or -all for all generated batchId-s\n" +
      "\t-crawlId <id>\t the id to prefix the schemas to operate on, (default: storage.crawl.id)\n" +
      "\t-threads N\tnumber of fetching threads per task\n" +
      "\t-parse\tif specified then fetcher will immediately parse fetched content\n" +
      "\t-resume\tresume interrupted job\n" +
      "\t-numTasks N\tif N > 0 then use this many reduce tasks for fetching (default: mapred.map.tasks)";

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
      } else if ("-parse".equals(args[i])) {
        parse = true;
      } else if ("-numTasks".equals(args[i])) {
        numTasks = Integer.parseInt(args[++i]);
      } else if ("-crawlId".equals(args[i])) {
        getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
      }
    }

    int fetchcode = fetch(batchId, threads, shouldResume, parse, numTasks); // run the Fetcher

    return fetchcode;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new FetcherJob(), args);
    System.exit(res);
  }
}

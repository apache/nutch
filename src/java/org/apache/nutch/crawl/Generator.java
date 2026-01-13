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
package org.apache.nutch.crawl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.conf.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.MapContext;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.nutch.hostdb.HostDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.metrics.NutchMetrics;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.JexlUtil;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.SegmentReaderUtil;
import org.apache.nutch.util.URLUtil;

/**
 * Generates a subset of a CrawlDb to fetch. This version allows to generate
 * fetchlists for several segments in one go. Unlike in the initial version
 * (OldGenerator), the IP resolution is done ONLY on the entries which have been
 * selected for fetching. The URLs are partitioned by IP, domain or host within
 * a segment. We can choose separately how to count the URLs i.e. by domain or
 * host to limit the entries.
 * 
 * <h2>HostDb Integration (NUTCH-2455)</h2>
 * <p>
 * When configured with a HostDb (via {@code -hostdb} option or 
 * {@code generate.hostdb} property), the Generator can apply per-host settings
 * using JEXL expressions:
 * </p>
 * <ul>
 *   <li>{@code generate.max.count.expr} - Expression to compute max URLs per host</li>
 *   <li>{@code generate.fetch.delay.expr} - Expression to compute fetch delay per host</li>
 * </ul>
 * 
 * <h3>Performance Characteristics</h3>
 * <p>
 * The HostDb integration uses secondary sorting via MapReduce to efficiently
 * merge HostDb entries with CrawlDb entries. This approach has the following
 * performance benefits compared to loading the entire HostDb into memory:
 * </p>
 * <ul>
 *   <li><b>Memory efficiency:</b> HostDb entries are streamed through the reducer
 *       rather than cached entirely in memory. Only per-host metadata is retained
 *       during URL processing.</li>
 *   <li><b>Scalability:</b> Can handle HostDb with millions of hosts without
 *       running out of heap space in the reducer.</li>
 *   <li><b>I/O efficiency:</b> Uses Hadoop's sorted merge join pattern, leveraging
 *       disk-based sorting and sequential reads.</li>
 * </ul>
 * 
 * <h3>Backward Compatibility</h3>
 * <p>
 * When {@code generate.hostdb} is not configured, the Generator operates without
 * HostDb integration, using only the default {@code generate.max.count} setting.
 * JEXL expressions ({@code generate.max.count.expr}, {@code generate.fetch.delay.expr})
 * are only evaluated when a HostDb is provided.
 * </p>
 * 
 * @see org.apache.nutch.hostdb.UpdateHostDb
 * @see org.apache.nutch.hostdb.HostDatum
 **/
public class Generator extends NutchTool implements Tool {

  private static final Random RANDOM = new Random();

  protected static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static final String GENERATE_UPDATE_CRAWLDB = "generate.update.crawldb";
  public static final String GENERATOR_MIN_SCORE = "generate.min.score";
  public static final String GENERATOR_MIN_INTERVAL = "generate.min.interval";
  public static final String GENERATOR_RESTRICT_STATUS = "generate.restrict.status";
  public static final String GENERATOR_FILTER = "generate.filter";
  public static final String GENERATOR_NORMALISE = "generate.normalise";
  public static final String GENERATOR_MAX_COUNT = "generate.max.count";
  public static final String GENERATOR_COUNT_MODE = "generate.count.mode";
  public static final String GENERATOR_COUNT_VALUE_DOMAIN = "domain";
  public static final String GENERATOR_COUNT_VALUE_HOST = "host";
  public static final String GENERATOR_TOP_N = "generate.topN";
  public static final String GENERATOR_CUR_TIME = "generate.curTime";
  public static final String GENERATOR_DELAY = "crawl.gen.delay";
  public static final String GENERATOR_MAX_NUM_SEGMENTS = "generate.max.num.segments";
  public static final String GENERATOR_EXPR = "generate.expr";
  public static final String GENERATOR_HOSTDB = "generate.hostdb";
  public static final String GENERATOR_MAX_COUNT_EXPR = "generate.max.count.expr";
  public static final String GENERATOR_FETCH_DELAY_EXPR = "generate.fetch.delay.expr";

  /**
   * Selector entry holds URL, CrawlDatum, segment number, and optionally HostDatum.
   * Used to carry data through the MapReduce pipeline.
   */
  public static class SelectorEntry implements Writable {
    public Text url;
    public CrawlDatum datum;
    public IntWritable segnum;
    public HostDatum hostdatum;

    public SelectorEntry() {
      url = new Text();
      datum = new CrawlDatum();
      segnum = new IntWritable(0);
      hostdatum = new HostDatum();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      url.readFields(in);
      datum.readFields(in);
      segnum.readFields(in);
      hostdatum.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      url.write(out);
      datum.write(out);
      segnum.write(out);
      hostdatum.write(out);
    }

    @Override
    public String toString() {
      return "url=" + url.toString() + ", datum=" + datum.toString()
          + ", segnum=" + segnum.toString();
    }
  }

  /**
   * Composite key for secondary sorting. Contains score and hostname.
   * Used to ensure HostDb entries arrive before CrawlDb entries in the reducer.
   * 
   * For HostDb entries: FloatTextPair(-Float.MAX_VALUE, hostname)
   * For CrawlDb entries: FloatTextPair(score, "")
   */
  public static class FloatTextPair implements WritableComparable<FloatTextPair> {
    public FloatWritable first;
    public Text second;

    public FloatTextPair() {
      this.first = new FloatWritable();
      this.second = new Text();
    }

    public FloatTextPair(FloatWritable first, Text second) {
      this.first = first;
      this.second = second;
    }

    public FloatTextPair(float first, String second) {
      this.first = new FloatWritable(first);
      this.second = new Text(second);
    }

    public FloatWritable getFirst() {
      return first;
    }

    public void setFirst(FloatWritable first) {
      this.first = first;
    }

    public Text getSecond() {
      return second;
    }

    public void setSecond(Text second) {
      this.second = second;
    }

    public void set(FloatWritable first, Text second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public int hashCode() {
      return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof FloatTextPair) {
        FloatTextPair tp = (FloatTextPair) obj;
        return first.equals(tp.getFirst()) && second.equals(tp.getSecond());
      }
      return false;
    }

    @Override
    public String toString() {
      return first + "\t" + second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      first.readFields(in);
      second.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      first.write(out);
      second.write(out);
    }

    @Override
    public int compareTo(FloatTextPair tp) {
      int cmp = first.compareTo(tp.getFirst());
      if (cmp != 0)
        return cmp;
      return second.compareTo(tp.getSecond());
    }
  }

  /**
   * Comparator that ensures HostDb entries (with hostname in second field)
   * sort before CrawlDb entries (with empty second field) for the same host.
   * This enables the secondary sorting pattern for NUTCH-2455.
   */
  public static class ScoreHostKeyComparator extends WritableComparator {
    protected ScoreHostKeyComparator() {
      super(FloatTextPair.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      FloatTextPair key1 = (FloatTextPair) w1;
      FloatTextPair key2 = (FloatTextPair) w2;

      boolean isKey1HostDatum = key1.second.getLength() > 0;
      boolean isKey2HostDatum = key2.second.getLength() > 0;

      if (isKey1HostDatum && isKey2HostDatum) {
        // Both are HostDb entries, sort by hostname
        return key1.second.compareTo(key2.second);
      } else {
        if (isKey1HostDatum == isKey2HostDatum) {
          // Both are CrawlDb entries, sort by score descending
          return -1 * key1.first.compareTo(key2.first);
        } else if (isKey1HostDatum) {
          // HostDb entries come before CrawlDb entries
          return -1;
        } else {
          return 1;
        }
      }
    }
  }

  /**
   * Mapper that reads HostDb and emits entries for secondary sorting.
   * Uses a very low score (-Float.MAX_VALUE) to ensure HostDb entries
   * sort before CrawlDb entries.
   */
  public static class HostDbReaderMapper
      extends Mapper<Text, HostDatum, FloatTextPair, SelectorEntry> {

    @Override
    public void map(Text hostname, HostDatum value, Context context)
        throws IOException, InterruptedException {
      SelectorEntry hostDataSelector = new SelectorEntry();
      try {
        hostDataSelector.hostdatum = (HostDatum) value.clone();
      } catch (CloneNotSupportedException e) {
        hostDataSelector.hostdatum = value;
      }

      // Use very low score and hostname to ensure HostDb entries
      // sort before CrawlDb entries for the same host
      context.write(new FloatTextPair(new FloatWritable(-Float.MAX_VALUE),
          hostname), hostDataSelector);
    }
  }

  /** Selects entries due for fetch. Partitions by host/domain/IP. */
  public static class Selector extends Partitioner<FloatTextPair, Writable>
      implements Configurable {

    private final URLPartitioner partitioner = new URLPartitioner();
    private Configuration conf;
    private int seed;
    private String mode = URLPartitioner.PARTITION_MODE_HOST;

    /**
     * Partition by host / domain or IP.
     * For HostDb entries (key.second is non-empty), partition by the hostname.
     * For CrawlDb entries (key.second is empty), use the URLPartitioner.
     * 
     * Note: When partition.url.mode is not "byHost", HostDb entries may not
     * be correctly grouped with their corresponding CrawlDb entries. For best
     * results, use "byHost" mode (the default) when using HostDb.
     */
    @Override
    public int getPartition(FloatTextPair key, Writable value,
        int numReduceTasks) {
      SelectorEntry entry = (SelectorEntry) value;
      // For HostDb entries, use the hostname from the key
      // For CrawlDb entries, use the URL from the entry
      if (key.second.getLength() > 0) {
        // HostDb entry - partition by hostname
        // This works best with "byHost" mode; other modes may cause
        // HostDb entries to not be grouped with their CrawlDb entries
        int hashCode = key.second.toString().hashCode();
        hashCode ^= seed;
        return (hashCode & Integer.MAX_VALUE) % numReduceTasks;
      }
      return partitioner.getPartition(entry.url, key.first, numReduceTasks);
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      this.seed = conf.getInt("partition.url.seed", 0);
      this.mode = conf.get(URLPartitioner.PARTITION_MODE_KEY, 
          URLPartitioner.PARTITION_MODE_HOST);
      partitioner.setConf(conf);
      
      // Warn if mode is not byHost and hostdb is configured
      String hostdb = conf.get(GENERATOR_HOSTDB);
      if (hostdb != null && !mode.equals(URLPartitioner.PARTITION_MODE_HOST)) {
        LOG.warn("HostDb is configured but partition.url.mode is '{}'. " +
            "For correct HostDb integration, use 'byHost' mode (the default). " +
            "Other modes may cause HostDb entries to not be grouped with their CrawlDb entries.",
            mode);
      }
    }
  }

  /** Select and invert subset due for fetch. */
  public static class SelectorMapper
      extends Mapper<Text, CrawlDatum, FloatTextPair, SelectorEntry> {

    private LongWritable genTime = new LongWritable(System.currentTimeMillis());
    private long curTime;
    private Configuration conf;
    private URLFilters filters;
    private ScoringFilters scfilters;
    private SelectorEntry entry = new SelectorEntry();
    private FloatWritable sortValue = new FloatWritable();
    private boolean filter;
    private long genDelay;
    private FetchSchedule schedule;
    private float scoreThreshold = 0f;
    private int intervalThreshold = -1;
    private byte restrictStatus = -1;
    private JexlScript expr = null;

    @Override
    public void setup(Context context) throws IOException {
      conf = context.getConfiguration();
      curTime = conf.getLong(GENERATOR_CUR_TIME, System.currentTimeMillis());
      filters = new URLFilters(conf);
      scfilters = new ScoringFilters(conf);
      filter = conf.getBoolean(GENERATOR_FILTER, true);
      /* CrawlDb items are unblocked after 7 days as default */
      genDelay = conf.getLong(GENERATOR_DELAY, 604800000L);
      long time = conf.getLong(Nutch.GENERATE_TIME_KEY, 0L);
      if (time > 0)
        genTime.set(time);
      schedule = FetchScheduleFactory.getFetchSchedule(conf);
      scoreThreshold = conf.getFloat(GENERATOR_MIN_SCORE, Float.NaN);
      intervalThreshold = conf.getInt(GENERATOR_MIN_INTERVAL, -1);
      String restrictStatusString = conf.getTrimmed(GENERATOR_RESTRICT_STATUS,
          "");
      if (!restrictStatusString.isEmpty()) {
        restrictStatus = CrawlDatum.getStatusByName(restrictStatusString);
      }
      expr = JexlUtil.parseExpression(conf.get(GENERATOR_EXPR, null));
    }

    @Override
    public void map(Text key, CrawlDatum value, Context context)
        throws IOException, InterruptedException {
      Text url = key;
      if (filter) {
        // If filtering is on don't generate URLs that don't pass
        // URLFilters
        try {
          if (filters.filter(url.toString()) == null) {
            context.getCounter(NutchMetrics.GROUP_GENERATOR,
                NutchMetrics.GENERATOR_URL_FILTERS_REJECTED_TOTAL).increment(1);
            return;
          }
        } catch (URLFilterException e) {
          context.getCounter(NutchMetrics.GROUP_GENERATOR,
              NutchMetrics.GENERATOR_URL_FILTER_EXCEPTION_TOTAL).increment(1);
          LOG.warn("Couldn't filter url: {} ({})", url, e.getMessage());
        }
      }
      CrawlDatum crawlDatum = value;

      // check fetch schedule
      if (!schedule.shouldFetch(url, crawlDatum, curTime)) {
        LOG.debug("-shouldFetch rejected '{}', fetchTime={}, curTime={}", url,
            crawlDatum.getFetchTime(), curTime);
        context.getCounter(NutchMetrics.GROUP_GENERATOR,
            NutchMetrics.GENERATOR_SCHEDULE_REJECTED_TOTAL).increment(1);
        return;
      }

      LongWritable oldGenTime = (LongWritable) crawlDatum.getMetaData()
          .get(Nutch.WRITABLE_GENERATE_TIME_KEY);
      if (oldGenTime != null) { // awaiting fetch & update
        if (oldGenTime.get() + genDelay > curTime) { // still wait for
          // update
          context.getCounter(NutchMetrics.GROUP_GENERATOR,
              NutchMetrics.GENERATOR_WAIT_FOR_UPDATE_TOTAL).increment(1);
          return;
        }
      }
      float sort = 1.0f;
      try {
        sort = scfilters.generatorSortValue(key, crawlDatum, sort);
      } catch (ScoringFilterException sfe) {
        LOG.warn("Couldn't filter generatorSortValue for {}: {}", key, sfe);
      }

      // check expr
      if (expr != null) {
        if (!crawlDatum.execute(expr, key.toString())) {
          context.getCounter(NutchMetrics.GROUP_GENERATOR,
              NutchMetrics.GENERATOR_EXPR_REJECTED_TOTAL).increment(1);
          return;
        }
      }

      if (restrictStatus != -1 && restrictStatus != crawlDatum.getStatus()) {
        context.getCounter(NutchMetrics.GROUP_GENERATOR,
            NutchMetrics.GENERATOR_STATUS_REJECTED_TOTAL).increment(1);
        return;
      }

      // consider only entries with a score superior to the threshold
      if (!Float.isNaN(scoreThreshold) && sort < scoreThreshold) {
        context.getCounter(NutchMetrics.GROUP_GENERATOR,
            NutchMetrics.GENERATOR_SCORE_TOO_LOW_TOTAL).increment(1);
        return;
      }

      // consider only entries with a retry (or fetch) interval lower than
      // threshold
      if (intervalThreshold != -1
          && crawlDatum.getFetchInterval() > intervalThreshold) {
        context.getCounter(NutchMetrics.GROUP_GENERATOR,
            NutchMetrics.GENERATOR_INTERVAL_REJECTED_TOTAL).increment(1);
        return;
      }

      // sort by decreasing score, using ScoreHostKeyComparator
      sortValue.set(sort);
      // record generation time
      crawlDatum.getMetaData().put(Nutch.WRITABLE_GENERATE_TIME_KEY, genTime);
      entry.datum = crawlDatum;
      entry.url = key;

      // CrawlDb entries have empty hostname (second field)
      context.write(new FloatTextPair(sortValue, new Text()), entry);
    }
  }

  /** Collect until limit is reached. */
  public static class SelectorReducer extends
      Reducer<FloatTextPair, SelectorEntry, FloatWritable, SelectorEntry> {

    private HashMap<String, MutablePair<HostDatum, int[]>> hostDomainCounts = new HashMap<>();
    private long count;
    private int currentsegmentnum = 1;
    private MultipleOutputs<FloatWritable, SelectorEntry> mos;
    private String outputFile;
    private long limit;
    private int segCounts[];
    private int maxNumSegments = 1;
    private int maxCount;
    private Configuration conf;
    private boolean byDomain = false;
    private URLNormalizers normalizers;
    private static boolean normalise;
    private JexlScript maxCountExpr = null;
    private JexlScript fetchDelayExpr = null;

    private JexlContext createContext(HostDatum datum) {
      JexlContext context = new MapContext();
      context.set("dnsFailures", datum.getDnsFailures());
      context.set("connectionFailures", datum.getConnectionFailures());
      context.set("unfetched", datum.getUnfetched());
      context.set("fetched", datum.getFetched());
      context.set("notModified", datum.getNotModified());
      context.set("redirTemp", datum.getRedirTemp());
      context.set("redirPerm", datum.getRedirPerm());
      context.set("gone", datum.getGone());
      context.set("conf", conf);

      // Set metadata variables
      if (datum.hasMetaData()) {
        for (Map.Entry<Writable, Writable> entry : datum.getMetaData()
            .entrySet()) {
          Object value = entry.getValue();

          if (value instanceof FloatWritable) {
            FloatWritable fvalue = (FloatWritable) value;
            Text tkey = (Text) entry.getKey();
            context.set(tkey.toString(), fvalue.get());
          }

          if (value instanceof IntWritable) {
            IntWritable ivalue = (IntWritable) value;
            Text tkey = (Text) entry.getKey();
            context.set(tkey.toString(), ivalue.get());
          }

          if (value instanceof Text) {
            Text tvalue = (Text) value;
            Text tkey = (Text) entry.getKey();
            context.set(tkey.toString().replace("-", "_"), tvalue.toString());
          }
        }
      }

      return context;
    }

    @Override
    public void setup(Context context) throws IOException {
      conf = context.getConfiguration();
      mos = new MultipleOutputs<FloatWritable, SelectorEntry>(context);
      Job job = Job.getInstance(conf, "Nutch Generator.SelectorReducer");
      limit = conf.getLong(GENERATOR_TOP_N, Long.MAX_VALUE)
          / job.getNumReduceTasks();
      maxNumSegments = conf.getInt(GENERATOR_MAX_NUM_SEGMENTS, 1);
      segCounts = new int[maxNumSegments];
      maxCount = conf.getInt(GENERATOR_MAX_COUNT, -1);
      if (maxCount == -1) {
        byDomain = false;
      }
      if (GENERATOR_COUNT_VALUE_DOMAIN.equals(conf.get(GENERATOR_COUNT_MODE)))
        byDomain = true;
      normalise = conf.getBoolean(GENERATOR_NORMALISE, true);
      if (normalise)
        normalizers = new URLNormalizers(conf,
            URLNormalizers.SCOPE_GENERATE_HOST_COUNT);

      if (conf.get(GENERATOR_HOSTDB) != null) {
        maxCountExpr = JexlUtil
            .parseExpression(conf.get(GENERATOR_MAX_COUNT_EXPR, null));
        fetchDelayExpr = JexlUtil
            .parseExpression(conf.get(GENERATOR_FETCH_DELAY_EXPR, null));
      }
    }

    @Override
    public void cleanup(Context context)
        throws IOException, InterruptedException {
      mos.close();
    }

    @Override
    public void reduce(FloatTextPair key, Iterable<SelectorEntry> values,
        Context context) throws IOException, InterruptedException {

      LongWritable variableFetchDelayWritable = null; // in millis
      Text variableFetchDelayKey = new Text("_variableFetchDelay_");
      // local variable maxCount may hold host-specific count set in HostDb
      int maxCount = this.maxCount;
      int[] hostDomainCount = null;
      HostDatum hostDatum = null;

      for (SelectorEntry entry : values) {
        Text url = entry.url;
        String urlString = url.toString();
        URL u = null;
        String hostorDomainName = null;

        // Check if this is a HostDb entry (hostname in key.second)
        if (key.second.getLength() > 0) {
          // This is a HostDb entry - store it for later use
          try {
            hostDatum = (HostDatum) entry.hostdatum.clone();
            hostDomainCounts.put(key.second.toString(),
                new MutablePair<HostDatum, int[]>(hostDatum, new int[] { 1, 0 }));
          } catch (Exception e) {
            LOG.info("Exception while storing hostdb entry: {}", e.toString());
          }
          // Don't process HostDb entries as URLs
          continue;
        }

        // Process normal CrawlDb entry
        try {
          u = new URL(urlString);

          if (byDomain) {
            hostorDomainName = URLUtil.getUrlRootByMode(u,
                URLPartitioner.PARTITION_MODE_DOMAIN).toLowerCase();
          } else {
            hostorDomainName = URLUtil.getUrlRootByMode(u,
                URLPartitioner.PARTITION_MODE_HOST).toLowerCase();
          }

          MutablePair<HostDatum, int[]> hostDomainCountPair = hostDomainCounts
              .get(hostorDomainName);

          if (hostDomainCountPair == null) {
            hostDomainCount = new int[] { 1, 0 };
            hostDomainCountPair = new MutablePair<HostDatum, int[]>(null,
                hostDomainCount);
            hostDomainCounts.put(hostorDomainName, hostDomainCountPair);
          } else {
            hostDomainCount = hostDomainCountPair.getRight();
          }

          // Check hostdb expressions only for host, ignore domains
          if (!byDomain)
            hostDatum = hostDomainCountPair.getLeft();

          if (hostDatum != null) {
            if (maxCountExpr != null) {
              try {
                Object result = maxCountExpr.execute(createContext(hostDatum));
                long variableMaxCount = ((Number) result).longValue();
                LOG.debug("Generator: variable maxCount: {} for {}",
                    variableMaxCount, hostorDomainName);
                maxCount = (int) variableMaxCount;
              } catch (Exception e) {
                LOG.error(
                    "Unable to execute variable maxCount expression: {}",
                    e.getMessage());
              }
            }

            if (fetchDelayExpr != null) {
              try {
                Object result = fetchDelayExpr.execute(createContext(hostDatum));
                long variableFetchDelay = ((Number) result).longValue();
                LOG.debug("Generator: variable fetchDelay: {} ms for {}",
                    variableFetchDelay, hostorDomainName);
                variableFetchDelayWritable = new LongWritable(variableFetchDelay);
              } catch (Exception e) {
                LOG.error(
                    "Unable to execute fetch delay expression: {}",
                    e.getMessage());
              }
            }
          }
        } catch (UnknownHostException e) {
          LOG.warn("Unknown host for URL: {}", urlString);
          continue;
        } catch (MalformedURLException e) {
          LOG.warn("Malformed URL: '{}', skipping ({})", urlString,
              StringUtils.stringifyException(e));
          context.getCounter(NutchMetrics.GROUP_GENERATOR,
              NutchMetrics.GENERATOR_MALFORMED_URL_TOTAL).increment(1);
          continue;
        }

        if (maxCount == 0) {
          continue;
        }

        // Got a non-zero variable fetch delay? Add it to the datum's metadata
        if (variableFetchDelayWritable != null) {
          entry.datum.getMetaData().put(variableFetchDelayKey,
              variableFetchDelayWritable);
        }

        if (count == limit) {
          // do we have any segments left?
          if (currentsegmentnum < maxNumSegments) {
            count = 0;
            currentsegmentnum++;
          } else
            break;
        }

        try {
          if (normalise && normalizers != null) {
            urlString = normalizers.normalize(urlString,
                URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
          }
        } catch (MalformedURLException e) {
          LOG.warn("Malformed URL: '{}', skipping ({})", urlString,
              StringUtils.stringifyException(e));
          context.getCounter(NutchMetrics.GROUP_GENERATOR,
              NutchMetrics.GENERATOR_MALFORMED_URL_TOTAL).increment(1);
          continue;
        }

        // only filter if we are counting hosts or domains
        if (maxCount > 0) {
          // increment hostCount
          hostDomainCount[1]++;

          // check if topN reached, select next segment if it is
          while (segCounts[hostDomainCount[0] - 1] >= limit
              && hostDomainCount[0] < maxNumSegments) {
            hostDomainCount[0]++;
            hostDomainCount[1] = 0;
          }

          // reached the limit of allowed URLs per host / domain
          // see if we can put it in the next segment?
          if (hostDomainCount[1] > maxCount) {
            if (hostDomainCount[0] < maxNumSegments) {
              hostDomainCount[0]++;
              hostDomainCount[1] = 1;
            } else {
              if (hostDomainCount[1] == (maxCount + 1)) {
                context.getCounter(NutchMetrics.GROUP_GENERATOR,
                    NutchMetrics.GENERATOR_HOSTS_AFFECTED_PER_HOST_OVERFLOW_TOTAL).increment(1);
                LOG.info(
                    "Host or domain {} has more than {} URLs for all {} segments. Additional URLs won't be included in the fetchlist.",
                    hostorDomainName, maxCount, maxNumSegments);
              }
              // skip this entry
              context.getCounter(NutchMetrics.GROUP_GENERATOR,
                  NutchMetrics.GENERATOR_URLS_SKIPPED_PER_HOST_OVERFLOW_TOTAL).increment(1);
              continue;
            }
          }
          entry.segnum = new IntWritable(hostDomainCount[0]);
          segCounts[hostDomainCount[0] - 1]++;
        } else {
          entry.segnum = new IntWritable(currentsegmentnum);
          segCounts[currentsegmentnum - 1]++;
        }

        outputFile = generateFileName(entry);
        mos.write("sequenceFiles", key.first, entry, outputFile);

        // Count is incremented only when we keep the URL
        // maxCount may cause us to skip it.
        count++;
      }
    }

    private String generateFileName(SelectorEntry entry) {
      return "fetchlist-" + entry.segnum.toString() + "/part";
    }
  }

  public static class DecreasingFloatComparator
      extends FloatWritable.Comparator {

    /** Compares two FloatWritables decreasing. */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return super.compare(b2, s2, l2, b1, s1, l1);
    }
  }

  public static class SelectorInverseMapper
      extends Mapper<FloatWritable, SelectorEntry, Text, SelectorEntry> {

    @Override
    public void map(FloatWritable key, SelectorEntry value, Context context)
        throws IOException, InterruptedException {
      SelectorEntry entry = value;
      context.write(entry.url, entry);
    }
  }

  public static class PartitionReducer
      extends Reducer<Text, SelectorEntry, Text, CrawlDatum> {

    @Override
    public void reduce(Text key, Iterable<SelectorEntry> values,
        Context context) throws IOException, InterruptedException {
      // if using HashComparator, we get only one input key in case of
      // hash collision so use only URLs from values
      for (SelectorEntry entry : values) {
        context.write(entry.url, entry.datum);
      }
    }

  }

  /** Sort fetch lists by hash of URL. */
  public static class HashComparator extends WritableComparator {
    public HashComparator() {
      super(Text.class);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      Text url1 = (Text) a;
      Text url2 = (Text) b;
      int hash1 = hash(url1.getBytes(), 0, url1.getLength());
      int hash2 = hash(url2.getBytes(), 0, url2.getLength());
      return (hash1 < hash2 ? -1 : (hash1 == hash2 ? 0 : 1));
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int hash1 = hash(b1, s1, l1);
      int hash2 = hash(b2, s2, l2);
      return (hash1 < hash2 ? -1 : (hash1 == hash2 ? 0 : 1));
    }

    private static int hash(byte[] bytes, int start, int length) {
      int hash = 1;
      // make later bytes more significant in hash code, so that sorting
      // by hashcode correlates less with by-host ordering.
      for (int i = length - 1; i >= 0; i--)
        hash = (31 * hash) + bytes[start + i];
      return hash;
    }
  }

  /**
   * Update the CrawlDB so that the next generate won't include the same URLs.
   */
  public static class CrawlDbUpdater {

    public static class CrawlDbUpdateMapper
        extends Mapper<Text, CrawlDatum, Text, CrawlDatum> {
      @Override
      public void map(Text key, CrawlDatum value, Context context)
          throws IOException, InterruptedException {
        context.write(key, value);
      }
    }

    public static class CrawlDbUpdateReducer
        extends Reducer<Text, CrawlDatum, Text, CrawlDatum> {

      private CrawlDatum orig = new CrawlDatum();
      private LongWritable genTime = new LongWritable(0L);
      private long generateTime;

      @Override
      public void setup(
          Reducer<Text, CrawlDatum, Text, CrawlDatum>.Context context) {
        Configuration conf = context.getConfiguration();
        generateTime = conf.getLong(Nutch.GENERATE_TIME_KEY, 0L);
      }

      @Override
      public void reduce(Text key, Iterable<CrawlDatum> values, Context context)
          throws IOException, InterruptedException {
        genTime.set(0L);
        for (CrawlDatum val : values) {
          if (val.getMetaData().containsKey(Nutch.WRITABLE_GENERATE_TIME_KEY)) {
            LongWritable gt = (LongWritable) val.getMetaData()
                .get(Nutch.WRITABLE_GENERATE_TIME_KEY);
            genTime.set(gt.get());
            if (genTime.get() != generateTime) {
              orig.set(val);
              genTime.set(0L);
              continue;
            }
          } else {
            orig.set(val);
          }
        }
        if (genTime.get() != 0L) {
          orig.getMetaData().put(Nutch.WRITABLE_GENERATE_TIME_KEY, genTime);
        }
        context.write(key, orig);
      }
    }
  }

  public Generator() {
  }

  public Generator(Configuration conf) {
    setConf(conf);
  }

  /**
   * @param dbDir
   *          Crawl database directory
   * @param segments
   *          Segments directory
   * @param numLists
   *          Number of fetch lists (partitions) per segment or number of
   *          fetcher map tasks. (One fetch list partition is fetched in one
   *          fetcher map task.)
   * @param topN
   *          Number of top URLs to be selected
   * @param curTime
   *          Current time in milliseconds
   * @return Path to generated segment or null if no entries were selected
   * @throws IOException
   *           if an I/O exception occurs.
   * @see LockUtil#createLockFile(Configuration, Path, boolean)
   * @throws InterruptedException
   *           if a thread is waiting, sleeping, or otherwise occupied, and the
   *           thread is interrupted, either before or during the activity.
   * @throws ClassNotFoundException
   *           if runtime class(es) are not available
   */
  public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
      long curTime)
      throws IOException, InterruptedException, ClassNotFoundException {

    Job job = Job.getInstance(getConf(), "Nutch Generator: generate from " + dbDir);
    Configuration conf = job.getConfiguration();
    boolean filter = conf.getBoolean(GENERATOR_FILTER, true);
    boolean normalise = conf.getBoolean(GENERATOR_NORMALISE, true);
    return generate(dbDir, segments, numLists, topN, curTime, filter, normalise,
        false, 1, null);
  }

  /**
   * This is an old signature used for compatibility - does not specify whether
   * or not to normalise and set the number of segments to 1
   * 
   * @param dbDir
   *          Crawl database directory
   * @param segments
   *          Segments directory
   * @param numLists
   *          Number of fetch lists (partitions) per segment or number of
   *          fetcher map tasks. (One fetch list partition is fetched in one
   *          fetcher map task.)
   * @param topN
   *          Number of top URLs to be selected
   * @param curTime
   *          Current time in milliseconds
   * @param filter
   *          whether to apply filtering operation
   * @param force
   *          if true, and the target lockfile exists, consider it valid. If
   *          false and the target file exists, throw an IOException.
   * @deprecated since 1.19 use
   *             {@link #generate(Path, Path, int, long, long, boolean, boolean, boolean, int, String, String)}
   *             or
   *             {@link #generate(Path, Path, int, long, long, boolean, boolean, boolean, int, String)}
   *             in the instance that no hostdb is available
   * @throws IOException
   *           if an I/O exception occurs.
   * @see LockUtil#createLockFile(Configuration, Path, boolean)
   * @throws InterruptedException
   *           if a thread is waiting, sleeping, or otherwise occupied, and the
   *           thread is interrupted, either before or during the activity.
   * @throws ClassNotFoundException
   *           if runtime class(es) are not available
   * @return Path to generated segment or null if no entries were selected
   **/
  @Deprecated
  public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
      long curTime, boolean filter, boolean force)
      throws IOException, InterruptedException, ClassNotFoundException {
    return generate(dbDir, segments, numLists, topN, curTime, filter, true,
        force, 1, null);
  }

  /**
   * This signature should be used in the instance that no hostdb is available.
   * Generate fetchlists in one or more segments. Whether to filter URLs or not
   * is read from the &quot;generate.filter&quot; property set for the job from
   * command-line. If the property is not found, the URLs are filtered. Same for
   * the normalisation.
   * 
   * @param dbDir
   *          Crawl database directory
   * @param segments
   *          Segments directory
   * @param numLists
   *          Number of fetch lists (partitions) per segment or number of
   *          fetcher map tasks. (One fetch list partition is fetched in one
   *          fetcher map task.)
   * @param topN
   *          Number of top URLs to be selected
   * @param curTime
   *          Current time in milliseconds
   * @param filter
   *          whether to apply filtering operation
   * @param norm
   *          whether to apply normalization operation
   * @param force
   *          if true, and the target lockfile exists, consider it valid. If
   *          false and the target file exists, throw an IOException.
   * @param maxNumSegments
   *          maximum number of segments to generate
   * @param expr
   *          a Jexl expression to use in the Generator job.
   * @see JexlUtil#parseExpression(String)
   * @throws IOException
   *           if an I/O exception occurs.
   * @see LockUtil#createLockFile(Configuration, Path, boolean)
   * @throws InterruptedException
   *           if a thread is waiting, sleeping, or otherwise occupied, and the
   *           thread is interrupted, either before or during the activity.
   * @throws ClassNotFoundException
   *           if runtime class(es) are not available
   * @return Path to generated segment or null if no entries were selected
   **/
  public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
      long curTime, boolean filter, boolean norm, boolean force,
      int maxNumSegments, String expr)
      throws IOException, InterruptedException, ClassNotFoundException {
    return generate(dbDir, segments, numLists, topN, curTime, filter, norm,
        force, maxNumSegments, expr, null);
  }

  /**
   * Generate fetchlists in one or more segments. Whether to filter URLs or not
   * is read from the &quot;generate.filter&quot; property set for the job from
   * command-line. If the property is not found, the URLs are filtered. Same for
   * the normalisation.
   * 
   * @param dbDir
   *          Crawl database directory
   * @param segments
   *          Segments directory
   * @param numLists
   *          Number of fetch lists (partitions) per segment or number of
   *          fetcher map tasks. (One fetch list partition is fetched in one
   *          fetcher map task.)
   * @param topN
   *          Number of top URLs to be selected
   * @param curTime
   *          Current time in milliseconds
   * @param filter
   *          whether to apply filtering operation
   * @param norm
   *          whether to apply normalization operation
   * @param force
   *          if true, and the target lockfile exists, consider it valid. If
   *          false and the target file exists, throw an IOException.
   * @param maxNumSegments
   *          maximum number of segments to generate
   * @param expr
   *          a Jexl expression to use in the Generator job.
   * @param hostdb
   *          name of a hostdb from which to execute Jexl expressions in a bid
   *          to determine the maximum URL count and/or fetch delay per host.
   * @see JexlUtil#parseExpression(String)
   * @throws IOException
   *           if an I/O exception occurs.
   * @see LockUtil#createLockFile(Configuration, Path, boolean)
   * @throws InterruptedException
   *           if a thread is waiting, sleeping, or otherwise occupied, and the
   *           thread is interrupted, either before or during the activity.
   * @throws ClassNotFoundException
   *           if runtime class(es) are not available
   * @return Path to generated segment or null if no entries were selected
   */
  public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
      long curTime, boolean filter, boolean norm, boolean force,
      int maxNumSegments, String expr, String hostdb)
      throws IOException, InterruptedException, ClassNotFoundException {

    Path tempDir = new Path(getConf().get("mapreduce.cluster.temp.dir", ".")
        + "/generate-temp-" + java.util.UUID.randomUUID().toString());
    FileSystem fs = tempDir.getFileSystem(getConf());

    Path lock = CrawlDb.lock(getConf(), dbDir, force);

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    LOG.info("Generator: starting");
    LOG.info("Generator: selecting best-scoring urls due for fetch.");
    LOG.info("Generator: filtering: {}", filter);
    LOG.info("Generator: normalizing: {}", norm);
    if (topN != Long.MAX_VALUE) {
      LOG.info("Generator: topN: {}", topN);
    }
    if (expr != null) {
      LOG.info("Generator: expr: {}", expr);
    }
    if (hostdb != null) {
      LOG.info("Generator: hostdb: {}", hostdb);
    }

    // map to inverted subset due for fetch, sort by score
    Job job = Job.getInstance(getConf(), "Nutch Generator: generate from " + dbDir);
    Configuration conf = job.getConfiguration();
    if (numLists == -1) {
      /* for politeness create exactly one partition per fetch task */ 
      numLists = Integer.parseInt(conf.get("mapreduce.job.maps"));
    }
    if ("local".equals(conf.get("mapreduce.framework.name")) && numLists != 1) {
      // override
      LOG.info(
          "Generator: running in local mode, generating exactly one partition.");
      numLists = 1;
    }
    conf.setLong(GENERATOR_CUR_TIME, curTime);
    // record real generation time
    long generateTime = System.currentTimeMillis();
    conf.setLong(Nutch.GENERATE_TIME_KEY, generateTime);
    conf.setLong(GENERATOR_TOP_N, topN);
    conf.setBoolean(GENERATOR_FILTER, filter);
    conf.setBoolean(GENERATOR_NORMALISE, norm);
    conf.setInt(GENERATOR_MAX_NUM_SEGMENTS, maxNumSegments);
    if (expr != null) {
      conf.set(GENERATOR_EXPR, expr);
    }
    if (hostdb != null) {
      conf.set(GENERATOR_HOSTDB, hostdb);
      // Use MultipleInputs to read from both HostDb and CrawlDb
      MultipleInputs.addInputPath(job, new Path(hostdb, "current"),
          SequenceFileInputFormat.class, HostDbReaderMapper.class);
    }
    // Add CrawlDb input
    MultipleInputs.addInputPath(job, new Path(dbDir, CrawlDb.CURRENT_NAME),
        SequenceFileInputFormat.class, SelectorMapper.class);

    job.setJarByClass(Selector.class);
    job.setMapOutputKeyClass(FloatTextPair.class);
    job.setMapOutputValueClass(SelectorEntry.class);
    job.setPartitionerClass(Selector.class);
    job.setReducerClass(SelectorReducer.class);
    // Use ScoreHostKeyComparator for secondary sorting
    job.setSortComparatorClass(ScoreHostKeyComparator.class);

    FileOutputFormat.setOutputPath(job, tempDir);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(SelectorEntry.class);
    MultipleOutputs.addNamedOutput(job, "sequenceFiles",
        SequenceFileOutputFormat.class, FloatWritable.class,
        SelectorEntry.class);

    try {
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = NutchJob.getJobFailureLogMessage("Generator", job);
        LOG.error(message);
        NutchJob.cleanupAfterFailure(tempDir, lock, fs);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("Generator job failed: {}", e.getMessage());
      NutchJob.cleanupAfterFailure(tempDir, lock, fs);
      throw e;
    }

    LOG.info("Generator: number of items rejected during selection:");
    for (Counter counter : job.getCounters().getGroup("Generator")) {
      LOG.info("Generator: {}  {}",
          String.format(Locale.ROOT, "%6d", counter.getValue()),
          counter.getName());
    }
    if (!getConf().getBoolean(GENERATE_UPDATE_CRAWLDB, false)) {
      /*
       * generated items are not marked in CrawlDb, and CrawlDb will not
       * accessed anymore: we already can release the lock
       */
      LockUtil.removeLockFile(getConf(), lock);
      lock = null;
    }

    // read the subdirectories generated in the temp
    // output and turn them into segments
    List<Path> generatedSegments = new ArrayList<>();

    FileStatus[] status = fs.listStatus(tempDir);
    try {
      for (FileStatus stat : status) {
        Path subfetchlist = stat.getPath();
        if (!subfetchlist.getName().startsWith("fetchlist-"))
          continue;
        // start a new partition job for this segment
        Path newSeg = partitionSegment(segments, subfetchlist, numLists);
        generatedSegments.add(newSeg);
      }
    } catch (Exception e) {
      LOG.warn("Generator: exception while partitioning segments, exiting ...");
      NutchJob.cleanupAfterFailure(tempDir, lock, fs);
      return null;
    }

    if (generatedSegments.size() == 0) {
      LOG.warn("Generator: 0 records selected for fetching, exiting ...");
      NutchJob.cleanupAfterFailure(tempDir, lock, fs);
      return null;
    }

    if (getConf().getBoolean(GENERATE_UPDATE_CRAWLDB, false)) {
      // update the db from tempDir
      Path tempDir2 = new Path(dbDir,
          "generate-temp-" + java.util.UUID.randomUUID().toString());

      job = Job.getInstance(getConf(), "Nutch Generator: updatedb " + dbDir);
      job.getConfiguration().setLong(Nutch.GENERATE_TIME_KEY, generateTime);
      for (Path segmpaths : generatedSegments) {
        Path subGenDir = new Path(segmpaths, CrawlDatum.GENERATE_DIR_NAME);
        FileInputFormat.addInputPath(job, subGenDir);
      }
      FileInputFormat.addInputPath(job, new Path(dbDir, CrawlDb.CURRENT_NAME));
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setMapperClass(CrawlDbUpdater.CrawlDbUpdateMapper.class);
      job.setReducerClass(CrawlDbUpdater.CrawlDbUpdateReducer.class);
      job.setJarByClass(CrawlDbUpdater.class);
      job.setOutputFormatClass(MapFileOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(CrawlDatum.class);
      FileOutputFormat.setOutputPath(job, tempDir2);
      try {
        boolean success = job.waitForCompletion(true);
        if (!success) {
          String message = NutchJob.getJobFailureLogMessage("Generator", job);
          LOG.error(message);
          NutchJob.cleanupAfterFailure(tempDir, lock, fs);
          NutchJob.cleanupAfterFailure(tempDir2, lock, fs);
          throw new RuntimeException(message);
        }
        CrawlDb.install(job, dbDir);
      } catch (IOException | InterruptedException | ClassNotFoundException e) {
        LOG.error("Generator job failed: {}", e.getMessage());
        NutchJob.cleanupAfterFailure(tempDir, lock, fs);
        NutchJob.cleanupAfterFailure(tempDir2, lock, fs);
        throw e;
      }

      fs.delete(tempDir2, true);
    }

    if (lock != null) {
      LockUtil.removeLockFile(getConf(), lock);
    }
    fs.delete(tempDir, true);

    stopWatch.stop();
    LOG.info("Generator: finished, elapsed: {} ms", stopWatch.getTime(
        TimeUnit.MILLISECONDS));

    Path[] patharray = new Path[generatedSegments.size()];
    return generatedSegments.toArray(patharray);
  }

  private Path partitionSegment(Path segmentsDir, Path inputDir, int numLists)
      throws IOException, ClassNotFoundException, InterruptedException {
    // invert again, partition by host/domain/IP, sort by url hash
    LOG.info("Generator: Partitioning selected urls for politeness.");

    Path segment = new Path(segmentsDir, generateSegmentName());
    Path output = new Path(segment, CrawlDatum.GENERATE_DIR_NAME);

    LOG.info("Generator: segment: {}", segment);

    Job job = Job.getInstance(getConf(), "Nutch Generator: partition segment " + segment);
    Configuration conf = job.getConfiguration();
    conf.setInt("partition.url.seed", RANDOM.nextInt());

    FileInputFormat.addInputPath(job, inputDir);
    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setJarByClass(Generator.class);
    job.setMapperClass(SelectorInverseMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(SelectorEntry.class);
    job.setPartitionerClass(URLPartitioner.class);
    job.setReducerClass(PartitionReducer.class);
    job.setNumReduceTasks(numLists);

    FileOutputFormat.setOutputPath(job, output);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);
    job.setSortComparatorClass(HashComparator.class);
    try {
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = NutchJob.getJobFailureLogMessage("Generator", job);
        LOG.error(message);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }

    return segment;
  }

  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

  public static synchronized String generateSegmentName() {
    try {
      Thread.sleep(1000);
    } catch (Throwable t) {
    }
    ;
    return sdf.format(new Date(System.currentTimeMillis()));
  }

  /**
   * Generate a fetchlist from the crawldb.
   * @param args array of arguments for this job
   * @throws Exception if there is an error running the job
   */
  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new Generator(),
        args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println(
          "Usage: Generator <crawldb> <segments_dir> [-hostdb <hostdb>] [-force] [-topN N] [-numFetchers numFetchers] [-expr <expr>] [-adddays <numDays>] [-noFilter] [-noNorm] [-maxNumSegments <num>]");
      return -1;
    }

    Path dbDir = new Path(args[0]);
    Path segmentsDir = new Path(args[1]);
    String hostdb = null;
    long curTime = System.currentTimeMillis();
    long topN = Long.MAX_VALUE;
    int numFetchers = -1;
    boolean filter = true;
    boolean norm = true;
    boolean force = false;
    String expr = null;
    int maxNumSegments = 1;

    for (int i = 2; i < args.length; i++) {
      if ("-topN".equals(args[i])) {
        topN = Long.parseLong(args[i + 1]);
        i++;
      } else if ("-numFetchers".equals(args[i])) {
        numFetchers = Integer.parseInt(args[i + 1]);
        i++;
      } else if ("-hostdb".equals(args[i])) {
        hostdb = args[i + 1];
        i++;
      } else if ("-adddays".equals(args[i])) {
        long numDays = Integer.parseInt(args[i + 1]);
        curTime += numDays * 1000L * 60 * 60 * 24;
      } else if ("-noFilter".equals(args[i])) {
        filter = false;
      } else if ("-noNorm".equals(args[i])) {
        norm = false;
      } else if ("-force".equals(args[i])) {
        force = true;
      } else if ("-maxNumSegments".equals(args[i])) {
        maxNumSegments = Integer.parseInt(args[i + 1]);
      } else if ("-expr".equals(args[i])) {
        expr = args[i + 1];
      }

    }

    try {
      Path[] segs = generate(dbDir, segmentsDir, numFetchers, topN, curTime,
          filter, norm, force, maxNumSegments, expr, hostdb);
      if (segs == null)
        return 1;
    } catch (Exception e) {
      LOG.error("Generator:", e);
      return -1;
    }
    return 0;
  }

  @Override
  public Map<String, Object> run(Map<String, Object> args, String crawlId)
      throws Exception {

    Map<String, Object> results = new HashMap<>();

    long curTime = System.currentTimeMillis();
    long topN = Long.MAX_VALUE;
    int numFetchers = -1;
    boolean filter = true;
    boolean norm = true;
    boolean force = false;
    int maxNumSegments = 1;
    String expr = null;
    String hostdb = null;
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

    Path segmentsDir;
    if (args.containsKey(Nutch.ARG_SEGMENTDIR)) {
      Object segDir = args.get(Nutch.ARG_SEGMENTDIR);
      if (segDir instanceof Path) {
        segmentsDir = (Path) segDir;
      } else {
        segmentsDir = new Path(segDir.toString());
      }
    } else {
      segmentsDir = new Path(crawlId + "/segments");
    }
    if (args.containsKey(Nutch.ARG_HOSTDB)) {
      hostdb = (String) args.get(Nutch.ARG_HOSTDB);
    }

    if (args.containsKey("expr")) {
      expr = (String) args.get("expr");
    }
    if (args.containsKey("topN")) {
      topN = Long.parseLong((String) args.get("topN"));
    }
    if (args.containsKey("numFetchers")) {
      numFetchers = Integer.parseInt((String) args.get("numFetchers"));
    }
    if (args.containsKey("adddays")) {
      long numDays = Integer.parseInt((String) args.get("adddays"));
      curTime += numDays * 1000L * 60 * 60 * 24;
    }
    if (args.containsKey("noFilter")) {
      filter = false;
    }
    if (args.containsKey("noNorm")) {
      norm = false;
    }
    if (args.containsKey("force")) {
      force = true;
    }
    if (args.containsKey("maxNumSegments")) {
      maxNumSegments = Integer.parseInt((String) args.get("maxNumSegments"));
    }

    try {
      Path[] segs = generate(crawlDb, segmentsDir, numFetchers, topN, curTime,
          filter, norm, force, maxNumSegments, expr, hostdb);
      if (segs == null) {
        results.put(Nutch.VAL_RESULT, Integer.toString(1));
        return results;
      }

    } catch (Exception e) {
      LOG.error("Generator: {}", StringUtils.stringifyException(e));
      results.put(Nutch.VAL_RESULT, Integer.toString(-1));
      return results;
    }
    results.put(Nutch.VAL_RESULT, Integer.toString(0));
    return results;
  }
}

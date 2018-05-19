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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.MapContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.nutch.hostdb.HostDatum;
import org.apache.nutch.metadata.Nutch;
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
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.URLUtil;

/**
 * Generates a subset of a crawl db to fetch. This version allows to generate
 * fetchlists for several segments in one go. Unlike in the initial version
 * (OldGenerator), the IP resolution is done ONLY on the entries which have been
 * selected for fetching. The URLs are partitioned by IP, domain or host within
 * a segment. We can chose separately how to count the URLS i.e. by domain or
 * host to limit the entries.
 **/
public class Generator extends NutchTool implements Tool {

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

  public static class SelectorEntry implements Writable {
    public Text url;
    public CrawlDatum datum;
    public IntWritable segnum;

    public SelectorEntry() {
      url = new Text();
      datum = new CrawlDatum();
      segnum = new IntWritable(0);
    }

    public void readFields(DataInput in) throws IOException {
      url.readFields(in);
      datum.readFields(in);
      segnum.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
      url.write(out);
      datum.write(out);
      segnum.write(out);
    }

    public String toString() {
      return "url=" + url.toString() + ", datum=" + datum.toString()
          + ", segnum=" + segnum.toString();
    }
  }

  /** Selects entries due for fetch. */
  public static class Selector extends
      Partitioner<FloatWritable, Writable> implements Configurable {

    private final URLPartitioner partitioner = new URLPartitioner();

    /** Partition by host / domain or IP. */
    public int getPartition(FloatWritable key, Writable value,
                            int numReduceTasks) {
      return partitioner.getPartition(((SelectorEntry) value).url, key,
              numReduceTasks);
    }

    @Override
    public Configuration getConf() {
      return partitioner.getConf();
    }

    @Override
    public void setConf(Configuration conf) {
      partitioner.setConf(conf);
    }
  }

    /** Select and invert subset due for fetch. */

    public static class SelectorMapper extends
       Mapper<Text, CrawlDatum, FloatWritable, SelectorEntry> {
  
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
      private String restrictStatus = null;
      private Expression expr = null;
 
      @Override 
      public void setup(Mapper<Text, CrawlDatum, FloatWritable, SelectorEntry>.Context context) throws IOException{
        conf = context.getConfiguration();
        curTime = conf.getLong(GENERATOR_CUR_TIME, System.currentTimeMillis());
        filters = new URLFilters(conf);
        scfilters = new ScoringFilters(conf);
        filter = conf.getBoolean(GENERATOR_FILTER, true);
        genDelay = conf.getLong(GENERATOR_DELAY, 7L) * 3600L * 24L * 1000L;
        long time = conf.getLong(Nutch.GENERATE_TIME_KEY, 0L);
        if (time > 0)
          genTime.set(time);
        schedule = FetchScheduleFactory.getFetchSchedule(conf);
        scoreThreshold = conf.getFloat(GENERATOR_MIN_SCORE, Float.NaN);
        intervalThreshold = conf.getInt(GENERATOR_MIN_INTERVAL, -1);
        restrictStatus = conf.get(GENERATOR_RESTRICT_STATUS, null);
        expr = JexlUtil.parseExpression(conf.get(GENERATOR_EXPR, null));
      }

      @Override 
      public void map(Text key, CrawlDatum value,
          	Context context)
          	throws IOException, InterruptedException {
        Text url = key;
        if (filter) {
          // If filtering is on don't generate URLs that don't pass
          // URLFilters
          try {
            if (filters.filter(url.toString()) == null)
              return;
          } catch (URLFilterException e) {
            if (LOG.isWarnEnabled()) {
              LOG.warn("Couldn't filter url: " + url + " (" + e.getMessage()
                  + ")");
            }
          }
        }
        CrawlDatum crawlDatum = value;

        // check fetch schedule
        if (!schedule.shouldFetch(url, crawlDatum, curTime)) {
          LOG.debug("-shouldFetch rejected '" + url + "', fetchTime="
              + crawlDatum.getFetchTime() + ", curTime=" + curTime);
          return;
        }

        LongWritable oldGenTime = (LongWritable) crawlDatum.getMetaData().get(
            Nutch.WRITABLE_GENERATE_TIME_KEY);
        if (oldGenTime != null) { // awaiting fetch & update
          if (oldGenTime.get() + genDelay > curTime) // still wait for
            // update
            return;
        }
        float sort = 1.0f;
        try {
          sort = scfilters.generatorSortValue(key, crawlDatum, sort);
        } catch (ScoringFilterException sfe) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Couldn't filter generatorSortValue for " + key + ": " + sfe);
          }
        }
        
        // check expr
        if (expr != null) {
          if (!crawlDatum.evaluate(expr, key.toString())) {
            return;
          }
        }

        if (restrictStatus != null
            && !restrictStatus.equalsIgnoreCase(CrawlDatum
                .getStatusName(crawlDatum.getStatus())))
          return;

        // consider only entries with a score superior to the threshold
        if (scoreThreshold != Float.NaN && sort < scoreThreshold)
          return;

        // consider only entries with a retry (or fetch) interval lower than
        // threshold
        if (intervalThreshold != -1
            && crawlDatum.getFetchInterval() > intervalThreshold)
          return;

        // sort by decreasing score, using DecreasingFloatComparator
        sortValue.set(sort);
        // record generation time
        crawlDatum.getMetaData().put(Nutch.WRITABLE_GENERATE_TIME_KEY, genTime);
        entry.datum = crawlDatum;
        entry.url = key;
        context.write(sortValue, entry); // invert for sort by score
      }
    }


    
    /** Collect until limit is reached. */
    public static class SelectorReducer extends
       Reducer<FloatWritable, SelectorEntry, FloatWritable, SelectorEntry> {
 
      private HashMap<String, int[]> hostCounts = new HashMap<>();
      private long count;
      private int currentsegmentnum = 1;
      private MultipleOutputs mos;
      private String outputFile;
      private long limit;
      private int segCounts[];
      private int maxNumSegments = 1;
      private int maxCount;
      private Configuration conf;
      private boolean byDomain = false;
      private URLNormalizers normalizers;
      private static boolean normalise;
      private MapFile.Reader[] hostdbReaders = null;
      private Expression maxCountExpr = null;
      private Expression fetchDelayExpr = null;

      public void open() {
        if (conf.get(GENERATOR_HOSTDB) != null) {
          try {
            Path path = new Path(conf.get(GENERATOR_HOSTDB), "current");
            hostdbReaders = MapFileOutputFormat.getReaders(path, conf);
          } catch (IOException e) {
            LOG.error("Error reading HostDB because {}", e.getMessage());
          }
        }
      }

      public void close() {
        if (hostdbReaders != null) {
          try {
            for (int i = 0; i < hostdbReaders.length; i++) {
              hostdbReaders[i].close();
            }
          } catch (IOException e) {
            LOG.error("Error closing HostDB because {}", e.getMessage());
          }
        }
      }

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
        for (Map.Entry<Writable, Writable> entry : datum.getMetaData().entrySet()) {
          Object value = entry.getValue();
          
          if (value instanceof FloatWritable) {
            FloatWritable fvalue = (FloatWritable)value;
            Text tkey = (Text)entry.getKey();
            context.set(tkey.toString(), fvalue.get());
          }
          
          if (value instanceof IntWritable) {
            IntWritable ivalue = (IntWritable)value;
            Text tkey = (Text)entry.getKey();
            context.set(tkey.toString(), ivalue.get());
          }

          if (value instanceof Text) {
            Text tvalue = (Text)value;
            Text tkey = (Text)entry.getKey();    
            context.set(tkey.toString().replace("-", "_"), tvalue.toString());
          }
        }

        return context;
      }

      @Override
      public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        mos = new MultipleOutputs(context);
        Job job = Job.getInstance(conf);
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
          maxCountExpr = JexlUtil.parseExpression(conf.get(GENERATOR_MAX_COUNT_EXPR, null));
          fetchDelayExpr = JexlUtil.parseExpression(conf.get(GENERATOR_FETCH_DELAY_EXPR, null));
        }
      }

      @Override
      public void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
      }

      @Override
      public void reduce(FloatWritable key, Iterable<SelectorEntry> values,
          Context context)
          throws IOException, InterruptedException {
      
        String hostname = null;
        HostDatum host = null;
        LongWritable variableFetchDelayWritable = null; // in millis
        Text variableFetchDelayKey = new Text("_variableFetchDelay_");
        int temp_maxCount = maxCount;
        for (SelectorEntry entry : values) {
          Text url = entry.url;
          String urlString = url.toString();
          URL u = null;
          
          // Do this only once per queue
          if (host == null) {
            try {
              hostname = URLUtil.getHost(urlString);
              host = getHostDatum(hostname);
            } catch (Exception e) {}
            
            // Got it?
            if (host == null) {
              // Didn't work, prevent future lookups
              host = new HostDatum();
            } else {
              if (maxCountExpr != null) {
                long variableMaxCount = Math.round((double)maxCountExpr.evaluate(createContext(host)));
                LOG.info("Generator: variable maxCount: {} for {}", variableMaxCount, hostname);
                maxCount = (int)variableMaxCount;
              }
              
              if (fetchDelayExpr != null) {
                long variableFetchDelay = Math.round((double)fetchDelayExpr.evaluate(createContext(host)));
                LOG.info("Generator: variable fetchDelay: {} ms for {}", variableFetchDelay, hostname);
                variableFetchDelayWritable = new LongWritable(variableFetchDelay);              
              }
            }
          }
          
          // Got a non-zero variable fetch delay? Add it to the datum's metadata
          if (variableFetchDelayWritable != null) {
            entry.datum.getMetaData().put(variableFetchDelayKey, variableFetchDelayWritable);
          }

          if (count == limit) {
            // do we have any segments left?
            if (currentsegmentnum < maxNumSegments) {
              count = 0;
              currentsegmentnum++;
            } else
              break;
          }

          String hostordomain = null;

          try {
            if (normalise && normalizers != null) {
              urlString = normalizers.normalize(urlString,
                  URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
            }
            u = new URL(urlString);
            if (byDomain) {
              hostordomain = URLUtil.getDomainName(u);
            } else {
              hostordomain = new URL(urlString).getHost();
            }
          } catch (Exception e) {
            LOG.warn("Malformed URL: '" + urlString + "', skipping ("
                + StringUtils.stringifyException(e) + ")");
            context.getCounter("Generator", "MALFORMED_URL").increment(1);
            continue;
          }

          hostordomain = hostordomain.toLowerCase();

          // only filter if we are counting hosts or domains
          if (maxCount > 0) {
            int[] hostCount = hostCounts.get(hostordomain);
            if (hostCount == null) {
              hostCount = new int[] { 1, 0 };
              hostCounts.put(hostordomain, hostCount);
            }

            // increment hostCount
            hostCount[1]++;

            // check if topN reached, select next segment if it is
            while (segCounts[hostCount[0] - 1] >= limit
                && hostCount[0] < maxNumSegments) {
              hostCount[0]++;
              hostCount[1] = 0;
            }

            // reached the limit of allowed URLs per host / domain
            // see if we can put it in the next segment?
            if (hostCount[1] >= maxCount) {
              if (hostCount[0] < maxNumSegments) {
                hostCount[0]++;
                hostCount[1] = 0;
              } else {
                if (hostCount[1] == maxCount + 1 && LOG.isInfoEnabled()) {
                  LOG.info("Host or domain "
                      + hostordomain
                      + " has more than "
                      + maxCount
                      + " URLs for all "
                      + maxNumSegments
                      + " segments. Additional URLs won't be included in the fetchlist.");
                }
                // skip this entry
                continue;
              }
            }
            entry.segnum = new IntWritable(hostCount[0]);
            segCounts[hostCount[0] - 1]++;
          } else {
            entry.segnum = new IntWritable(currentsegmentnum);
            segCounts[currentsegmentnum - 1]++;
          }

          outputFile = generateFileName(entry);
          mos.write("sequenceFiles", key, entry, outputFile);
          context.write(key,entry);

          // Count is incremented only when we keep the URL
          // maxCount may cause us to skip it.
          count++;
        }
        maxCount = temp_maxCount;
      }
      
      private String generateFileName(SelectorEntry entry) {
        return "fetchlist-" + entry.segnum.toString() + "/part";
      }

      private HostDatum getHostDatum(String host) throws Exception {
        Text key = new Text();
        HostDatum value = new HostDatum();

        open();
        for (int i = 0; i < hostdbReaders.length; i++) {
          while (hostdbReaders[i].next(key, value)) {
            if (host.equals(key.toString())) {
              close();
              return value;
            }
          }
        }

        close();
        return null;
      }
    }

  public static class DecreasingFloatComparator extends
      FloatWritable.Comparator {

    /** Compares two FloatWritables decreasing. */
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return super.compare(b2, s2, l2, b1, s1, l1);
    }
  }

  public static class SelectorInverseMapper extends
      Mapper<FloatWritable, SelectorEntry, Text, SelectorEntry> {

    public void map(FloatWritable key, SelectorEntry value,
        Context context)
        throws IOException, InterruptedException {
      SelectorEntry entry = value;
      context.write(entry.url, entry);
    }
  }

  public static class PartitionReducer extends
      Reducer<Text, SelectorEntry, Text, CrawlDatum> {

    public void reduce(Text key, Iterable<SelectorEntry> values,
        Context context)
        throws IOException, InterruptedException {
      // if using HashComparator, we get only one input key in case of
      // hash collision
      // so use only URLs from values
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
    public int compare(WritableComparable a, WritableComparable b) {
      Text url1 = (Text) a;
      Text url2 = (Text) b;
      int hash1 = hash(url1.getBytes(), 0, url1.getLength());
      int hash2 = hash(url2.getBytes(), 0, url2.getLength());
      return (hash1 < hash2 ? -1 : (hash1 == hash2 ? 0 : 1));
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int hash1 = hash(b1, s1, l1);
      int hash2 = hash(b2, s2, l2);
      return (hash1 < hash2 ? -1 : (hash1 == hash2 ? 0 : 1));
    }

    private static int hash(byte[] bytes, int start, int length) {
      int hash = 1;
      // make later bytes more significant in hash code, so that sorting
      // by
      // hashcode correlates less with by-host ordering.
      for (int i = length - 1; i >= 0; i--)
        hash = (31 * hash) + (int) bytes[start + i];
      return hash;
    }
  }

  /**
   * Update the CrawlDB so that the next generate won't include the same URLs.
   */
  public static class CrawlDbUpdater {

    public static class CrawlDbUpdateMapper extends
            Mapper<Text, CrawlDatum, Text, CrawlDatum> {
      @Override
      public void map(Text key, CrawlDatum value,
                      Context context)
              throws IOException, InterruptedException {
        context.write(key, value);
      }
    }

    public static class CrawlDbUpdateReducer extends
            Reducer<Text, CrawlDatum, Text, CrawlDatum> {

      private CrawlDatum orig = new CrawlDatum();
      private LongWritable genTime = new LongWritable(0L);
      private long generateTime;

      @Override
      public void setup(Reducer<Text, CrawlDatum, Text, CrawlDatum>.Context context) {
        Configuration conf = context.getConfiguration();
        generateTime = conf.getLong(Nutch.GENERATE_TIME_KEY, 0L);
      }

      @Override
      public void reduce(Text key, Iterable<CrawlDatum> values,
                         Context context)
              throws IOException, InterruptedException {
        genTime.set(0L);
        for (CrawlDatum val : values) {
          if (val.getMetaData().containsKey(Nutch.WRITABLE_GENERATE_TIME_KEY)) {
            LongWritable gt = (LongWritable) val.getMetaData().get(
                    Nutch.WRITABLE_GENERATE_TIME_KEY);
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

  public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
      long curTime) throws IOException, InterruptedException, ClassNotFoundException {

    Job job = NutchJob.getInstance(getConf());
    Configuration conf = job.getConfiguration();
    boolean filter = conf.getBoolean(GENERATOR_FILTER, true);
    boolean normalise = conf.getBoolean(GENERATOR_NORMALISE, true);
    return generate(dbDir, segments, numLists, topN, curTime, filter,
        normalise, false, 1, null);
  }

  /**
   * old signature used for compatibility - does not specify whether or not to
   * normalise and set the number of segments to 1
   **/
  public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
      long curTime, boolean filter, boolean force) throws IOException, InterruptedException, ClassNotFoundException {
    return generate(dbDir, segments, numLists, topN, curTime, filter, true,
        force, 1, null);
  }
  
  public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
      long curTime, boolean filter, boolean norm, boolean force,
      int maxNumSegments, String expr) throws IOException, InterruptedException, ClassNotFoundException {
    return generate(dbDir, segments, numLists, topN, curTime, filter, true,
        force, 1, expr, null);
  }

  /**
   * Generate fetchlists in one or more segments. Whether to filter URLs or not
   * is read from the crawl.generate.filter property in the configuration files.
   * If the property is not found, the URLs are filtered. Same for the
   * normalisation.
   * 
   * @param dbDir
   *          Crawl database directory
   * @param segments
   *          Segments directory
   * @param numLists
   *          Number of reduce tasks
   * @param topN
   *          Number of top URLs to be selected
   * @param curTime
   *          Current time in milliseconds
   * 
   * @return Path to generated segment or null if no entries were selected
   * 
   * @throws IOException
   *           When an I/O error occurs
   */
  public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
      long curTime, boolean filter, boolean norm, boolean force,
      int maxNumSegments, String expr, String hostdb) throws IOException, InterruptedException, ClassNotFoundException {

    Path tempDir = new Path(getConf().get("mapreduce.cluster.temp.dir", ".")
        + "/generate-temp-" + java.util.UUID.randomUUID().toString());
    FileSystem fs = tempDir.getFileSystem(getConf());

    Path lock = CrawlDb.lock(getConf(), dbDir, force);
    
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("Generator: starting at " + sdf.format(start));
    LOG.info("Generator: Selecting best-scoring urls due for fetch.");
    LOG.info("Generator: filtering: " + filter);
    LOG.info("Generator: normalizing: " + norm);
    if (topN != Long.MAX_VALUE) {
      LOG.info("Generator: topN: {}", topN);
    }
    if (expr != null) {
      LOG.info("Generator: expr: {}", expr);
    }
    if (expr != null) {
      LOG.info("Generator: hostdb: {}", hostdb);
    }
    
    // map to inverted subset due for fetch, sort by score
    Job job = NutchJob.getInstance(getConf());
    job.setJobName("generate: select from " + dbDir);
    Configuration conf = job.getConfiguration();
    if (numLists == -1) { // for politeness make
      numLists = Integer.parseInt(conf.get("mapreduce.job.maps")); // a partition per fetch task
    }
    if ("local".equals(conf.get("mapreduce.framework.name")) && numLists != 1) {
      // override
      LOG.info("Generator: running in local mode, generating exactly one partition.");
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
    }
    FileInputFormat.addInputPath(job, new Path(dbDir, CrawlDb.CURRENT_NAME));
    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setJarByClass(Selector.class);
    job.setMapperClass(SelectorMapper.class);
    job.setPartitionerClass(Selector.class);
    job.setReducerClass(SelectorReducer.class);

    FileOutputFormat.setOutputPath(job, tempDir);
    job.setOutputKeyClass(FloatWritable.class);
    job.setSortComparatorClass(DecreasingFloatComparator.class);
    job.setOutputValueClass(SelectorEntry.class);
    MultipleOutputs.addNamedOutput(job, "sequenceFiles", SequenceFileOutputFormat.class, FloatWritable.class, SelectorEntry.class);

    try {
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "Generator job did not succeed, job status:"
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        NutchJob.cleanupAfterFailure(tempDir, lock, fs);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("Generator job failed: {}", e.getMessage());
      NutchJob.cleanupAfterFailure(tempDir, lock, fs);
      throw e;
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
      fs.delete(tempDir, true);
      return null;
    }

    if (generatedSegments.size() == 0) {
      LOG.warn("Generator: 0 records selected for fetching, exiting ...");
      LockUtil.removeLockFile(getConf(), lock);
      fs.delete(tempDir, true);
      return null;
    }

    if (getConf().getBoolean(GENERATE_UPDATE_CRAWLDB, false)) {
      // update the db from tempDir
      Path tempDir2 = new Path(dbDir,
          "generate-temp-" + java.util.UUID.randomUUID().toString());

      job = NutchJob.getInstance(getConf());
      job.setJobName("generate: updatedb " + dbDir);
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
          String message = "Generator job did not succeed, job status:"
              + job.getStatus().getState() + ", reason: "
              + job.getStatus().getFailureInfo();
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

    LockUtil.removeLockFile(getConf(), lock);
    fs.delete(tempDir, true);

    long end = System.currentTimeMillis();
    LOG.info("Generator: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));

    Path[] patharray = new Path[generatedSegments.size()];
    return generatedSegments.toArray(patharray);
  }

  private Path partitionSegment(Path segmentsDir, Path inputDir, int numLists)
      throws IOException, ClassNotFoundException, InterruptedException {
    // invert again, partition by host/domain/IP, sort by url hash
    if (LOG.isInfoEnabled()) {
      LOG.info("Generator: Partitioning selected urls for politeness.");
    }
    Path segment = new Path(segmentsDir, generateSegmentName());
    Path output = new Path(segment, CrawlDatum.GENERATE_DIR_NAME);

    LOG.info("Generator: segment: " + segment);

    Job job = NutchJob.getInstance(getConf());
    job.setJobName("generate: partition " + segment);
    Configuration conf = job.getConfiguration();
    conf.setInt("partition.url.seed", new Random().nextInt());

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
        String message = "Generator job did not succeed, job status:"
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
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
   */
  public static void main(String args[]) throws Exception {
    int res = ToolRunner
        .run(NutchConfiguration.create(), new Generator(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.out
          .println("Usage: Generator <crawldb> <segments_dir> [-force] [-topN N] [-numFetchers numFetchers] [-expr <expr>] [-adddays <numDays>] [-noFilter] [-noNorm] [-maxNumSegments <num>]");
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
      LOG.error("Generator: " + StringUtils.stringifyException(e));
      return -1;
    }
    return 0;
  }

  @Override
  public Map<String, Object> run(Map<String, Object> args, String crawlId) throws Exception {

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
    }
    else {
      segmentsDir = new Path(crawlId+"/segments");
    }
    if (args.containsKey(Nutch.ARG_HOSTDB)) {
      	hostdb = (String)args.get(Nutch.ARG_HOSTDB);
    }
    
    if (args.containsKey("expr")) {
      expr = (String)args.get("expr");
    }
    if (args.containsKey("topN")) {
      topN = Long.parseLong((String)args.get("topN"));
    }
    if (args.containsKey("numFetchers")) {
      numFetchers = Integer.parseInt((String)args.get("numFetchers"));
    }
    if (args.containsKey("adddays")) {
      long numDays = Integer.parseInt((String)args.get("adddays"));
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
      maxNumSegments = Integer.parseInt((String)args.get("maxNumSegments"));
    }

    try {
      Path[] segs = generate(crawlDb, segmentsDir, numFetchers, topN, curTime,
          filter, norm, force, maxNumSegments, expr, hostdb);
      if (segs == null){
        results.put(Nutch.VAL_RESULT, Integer.toString(1));
        return results;
      }

    } catch (Exception e) {
      LOG.error("Generator: " + StringUtils.stringifyException(e));
      results.put(Nutch.VAL_RESULT, Integer.toString(-1));
      return results;
    }
    results.put(Nutch.VAL_RESULT, Integer.toString(0));
    return results;
  }
}

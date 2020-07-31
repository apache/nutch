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

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.hash.MurmurHash;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a subset of a crawl db to fetch.
 *
 * This version works differently from the original in that it doesn't try to
 * keep highest scoring things in earlier segments across domains.
 *
 * In order to speed up performance of generating dozens or hundreds of segments
 * for multi-billion entry URL databases, we group by host (or domain) and
 * secondary sort by score descending. No per-host (or per-domain) counts are
 * hold in memory. Grouping by IP is not supported.
 * 
 * If fetch lists are grouped by domain (see <code>generate.count.mode</code>)
 * additional limits are configurable (also per domain): max. number of hosts,
 * max. number of URLs per host, number of partitions URLs of a single domain
 * are distributed on.
 *
 * URLs of a single host or domain are distributed over multiple segments
 * round-robin but with a configurable number of URLs kept in a single segment
 * (see {@link #GENERATOR_COUNT_KEEP_MIN_IN_SEGMENT}). Keeping a minimum number
 * of same-host/domain URLs together minimizes the overhead caused by DNS
 * lookups and robots.txt fetches, parsing and storing robots.txt rules.
 * 
 * All segments are partitioned in a single job which saves time when many
 * segments (e.g., <code>-maxNumSegments 100</code>) are generated
 * ({@link Generator} launches one partition job per segment).
 **/
public class Generator2 extends Configured implements Tool {

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
  /**
   * When distributing URLs of the same host/domain over segments keep at least
   * this number of URLs together in one segment
   */
  public static final String GENERATOR_COUNT_KEEP_MIN_IN_SEGMENT = "generate.count.keep.min.urls.per.segment";
  /** Name of file with domain-specific limits */
  public static final String GENERATOR_DOMAIN_LIMITS_FILE = "generate.domain.limits.file";
  /** Max. number of hosts per domain (if generate.count.mode == domain) */
  public static final String GENERATOR_MAX_HOSTS_PER_DOMAIN = "generate.max.hosts.per.domain";
  /** Max. number of URLs per host (if generate.count.mode == domain) */
  public static final String GENERATOR_MAX_COUNT_PER_HOST = "generate.max.count.per.host.by.domain";

  protected static Random random = new Random();

  public static class DomainScorePair
      implements WritableComparable<DomainScorePair> {
    private Text domain = new Text();
    private FloatWritable score = new FloatWritable();

    public void set(String domain, float score) {
      this.domain.set(domain);
      this.score.set(score);
    }

    public Text getDomain() {
      return domain;
    }

    public FloatWritable getScore() {
      return score;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      domain.readFields(in);
      score.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      domain.write(out);
      score.write(out);
    }

    @Override
    public int hashCode() {
      return domain.hashCode() + score.hashCode();
    }

    @Override
    public boolean equals(Object right) {
      if (right instanceof DomainScorePair) {
        DomainScorePair r = (DomainScorePair) right;
        return r.domain.equals(domain) && r.score.equals(score);
      } else {
        return false;
      }
    }

    /* Sorts domain ascending, score in descending order */
    @Override
    public int compareTo(DomainScorePair o) {
      if (!domain.equals(o.getDomain())) {
        return domain.compareTo(o.getDomain());
      } else if (!score.equals(o.getScore())) {
        return o.getScore().compareTo(score);
      } else {
        return 0;
      }
    }
  }

  public static class DomainComparator extends WritableComparator {
    public DomainComparator() {
      super(DomainScorePair.class, true);
    }

    public int compare(DomainScorePair a, DomainScorePair b) {
      return a.getDomain().compareTo(b.getDomain());
    }

    @Override
    public int compare(Object a, Object b) {
      return compare((DomainScorePair) a, (DomainScorePair) b);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      return compare((DomainScorePair) a, (DomainScorePair) b);
    }
  }

  public static class ScoreComparator extends WritableComparator {
    public ScoreComparator() {
      super(DomainScorePair.class, true);
    }

    // Some versions of hadoop don't seem to have a FloatWritable.compareTo
    // also inverted for descending order
    public int compare(DomainScorePair a, DomainScorePair b) {
      return a.compareTo(b);
    }

    @Override
    public int compare(Object a, Object b) {
      return compare((DomainScorePair) a, (DomainScorePair) b);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      return compare((DomainScorePair) a, (DomainScorePair) b);
    }
  }

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

  /*
   * Takes the entire crawl db and filters down to those that are scheduled to
   * be output, have a high enough score and limits by host/domain
   */
  public static class Selector extends Partitioner<DomainScorePair, Writable>
      implements Configurable {

    private Configuration conf;
    private MurmurHash hasher = (MurmurHash) MurmurHash.getInstance();
    private int seed;

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      seed = conf.getInt("partition.url.seed", 0);
    }

    /**
     * Partition by host / domain, use MurmurHash because of better hashCode
     * distribution
     */
    @Override
    public int getPartition(DomainScorePair key, Writable value,
        int numReduceTasks) {
      Text domain = key.getDomain();
      /*
       * Note: the backing byte[] array of Text may include zero bytes or
       * garbage, must not use more than Text::getLength() bytes
       */
      return (hasher.hash(domain.getBytes(), domain.getLength(), seed)
          & Integer.MAX_VALUE) % numReduceTasks;
    }

  }

  public static class SelectorMapper
      extends Mapper<Text, CrawlDatum, DomainScorePair, SelectorEntry> {

    private Configuration conf;
    private LongWritable genTime = new LongWritable(System.currentTimeMillis());
    private long curTime;
    private boolean byDomain = false;
    private URLFilters filters;
    private URLNormalizers normalizers;
    private ScoringFilters scfilters;
    private SelectorEntry entry = new SelectorEntry();
    private boolean filter;
    private boolean normalise;
    private long genDelay;
    private FetchSchedule schedule;
    private float scoreThreshold = 0f;
    private int intervalThreshold = -1;
    private String restrictStatus = null;
    private DomainScorePair outputKey = new DomainScorePair();

    @Override
    public void setup(
        Mapper<Text, CrawlDatum, DomainScorePair, SelectorEntry>.Context context)
        throws IOException {
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

      if (GENERATOR_COUNT_VALUE_DOMAIN.equals(conf.get(GENERATOR_COUNT_MODE))) {
        byDomain = true;
      }
    }

    /** Select & invert subset due for fetch. */
    public void map(Text key, CrawlDatum value, Context context)
        throws IOException, InterruptedException {
      String urlString = key.toString();

      if (filter) {
        // If filtering is on don't generate URLs that don't pass
        // URLFilters
        try {
          if (filters.filter(urlString) == null)
            return;
        } catch (URLFilterException e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Couldn't filter url {}: {}", key, e.getMessage());
          }
        }
      }

      // check fetch schedule
      if (!schedule.shouldFetch(key, value, curTime)) {
        LOG.debug("-shouldFetch rejected '{}', fetchTime={}, curTime={}", key,
            value.getFetchTime(), curTime);
        context.getCounter("Schedule rejected by status",
            CrawlDatum.getStatusName(value.getStatus())).increment(1);
        return;
      }

      LongWritable oldGenTime = (LongWritable) value.getMetaData()
          .get(Nutch.WRITABLE_GENERATE_TIME_KEY);
      if (oldGenTime != null) { // awaiting fetch & update
        if (oldGenTime.get() + genDelay > curTime) // still wait for
          // update
          return;
      }
      float sort = 1.0f;
      try {
        sort = scfilters.generatorSortValue(key, value, sort);
      } catch (ScoringFilterException sfe) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Couldn't filter generatorSortValue for {}: {}", key, sfe);
        }
      }

      if (restrictStatus != null && !restrictStatus
          .equalsIgnoreCase(CrawlDatum.getStatusName(value.getStatus())))
        return;

      // consider only entries with a score superior to the threshold
      if (!Float.isNaN(scoreThreshold) && sort < scoreThreshold) {
        context.getCounter("Score below threshold by status",
            CrawlDatum.getStatusName(value.getStatus())).increment(1);
        return;
      }

      // consider only entries with a retry (or fetch) interval lower than
      // threshold
      if (intervalThreshold != -1
          && value.getFetchInterval() > intervalThreshold)
        return;

      String hostordomain;

      try {
        if (normalise && normalizers != null) {
          urlString = normalizers.normalize(urlString,
              URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
        }
        URL u = new URL(urlString);
        if (byDomain) {
          hostordomain = URLPartitioner.getDomainName(u.getHost());
        } else {
          hostordomain = u.getHost().toLowerCase(Locale.ROOT);
        }
      } catch (Exception e) {
        LOG.warn("Malformed URL: '{}', skipping ({})", urlString,
            e.getMessage());
        context.getCounter("Generator", "MALFORMED_URL").increment(1);
        return;
      }

      outputKey.set(hostordomain, sort);

      // record generation time
      value.getMetaData().put(Nutch.WRITABLE_GENERATE_TIME_KEY, genTime);
      entry.datum = value;
      entry.url = key;
      context.write(outputKey, entry);
    }

  }

  public static class SelectorReducer extends
      Reducer<DomainScorePair, SelectorEntry, FloatWritable, SelectorEntry> {

    private Configuration conf;
    private int maxCount;
    private int maxNumSegments = 1;
    private int currentSegment;
    private int keepMinUrlsPerSegment;
    private int segmentIncrement = 1;

    // additional host limits if byDomain
    private boolean byDomainWithHostLimits = false;
    private int maxCountPerHost = -1;
    private int maxHostsPerDomain = -1;
    private int partition = -1;
    private int numReduces = 1;
    private Selector selector = new Selector();
    private Map<String, DomainLimits> domainLimits = null;

    public static class DomainLimits {
      int maxURLs;
      int maxURLsPerHost;
      int maxHosts;
      int numPartitions;
      public DomainLimits(int urls, int urlsHost, int hosts, int parts) {
        maxURLs = urls;
        maxURLsPerHost = urlsHost;
        maxHosts = hosts;
        numPartitions = parts;
      }
      @Override
      public String toString() {
        if (numPartitions > 1) {
          return String.format(
              "max. urls = %d, urls/host = %d, hosts = %d, partitions = %d",
              maxURLs, maxURLsPerHost, maxHosts, numPartitions);
        } else {
          return String.format("max. urls = %d, urls/host = %d, hosts = %d",
              maxURLs, maxURLsPerHost, maxHosts);
        }
      }
    }

    @Override
    public void setup(Context context) throws IOException {
      conf = context.getConfiguration();
      maxNumSegments = conf.getInt(GENERATOR_MAX_NUM_SEGMENTS, 1);
      maxCount = conf.getInt(GENERATOR_MAX_COUNT, -1);
      partition = conf.getInt("mapreduce.task.partition", -1);
      numReduces = conf.getInt("mapreduce.job.reduces", 1);
      LOG.info("Selecting fetch lists for partition {} (out of {} partitions)",
          partition, numReduces);
      selector.setConf(conf);
      if (GENERATOR_COUNT_VALUE_DOMAIN.equals(conf.get(GENERATOR_COUNT_MODE))) {
        maxHostsPerDomain = conf.getInt(GENERATOR_MAX_HOSTS_PER_DOMAIN, -1);
        maxCountPerHost = conf.getInt(GENERATOR_MAX_COUNT_PER_HOST, -1);
        domainLimits = readLimitsFile(conf, partition, numReduces, selector);
        if (domainLimits != null || maxHostsPerDomain > 0
            || maxCountPerHost > 0) {
          byDomainWithHostLimits = true;
          LOG.info("Domain limits enabled:");
          LOG.info(" - {}: {}", GENERATOR_MAX_HOSTS_PER_DOMAIN,
              maxHostsPerDomain);
          LOG.info(" - {}: {}", GENERATOR_MAX_COUNT_PER_HOST,
              maxCountPerHost);
          if (domainLimits != null) {
            LOG.info(
                " - domain limits file ({}) contains limits for {} domains",
                GENERATOR_DOMAIN_LIMITS_FILE, domainLimits.size());
          }
        }
      }
      currentSegment = 0;
      keepMinUrlsPerSegment = conf.getInt(GENERATOR_COUNT_KEEP_MIN_IN_SEGMENT,
          100);
      segmentIncrement = 1; // increment to select next segment
      int prime[] = { 2, 3, 5, 7, 11, 13, 17, 23, 29 };
      // select next segment with a larger step, so that fetching of smaller
      // sites is paused between consecutive segments. Select a prime number
      // - which is not bigger than 1/3 of the number of segments
      // - number of segments isn't a multiple of
      for (int i = 0; i < prime.length; i++) {
        if (prime[i] >= (maxNumSegments / 3))
          break;
        if (0 == (maxNumSegments % prime[i]))
          continue;
        segmentIncrement = prime[i];
      }
    }

    private int nextSegment() {
      currentSegment += segmentIncrement;
      if (currentSegment >= maxNumSegments) {
        currentSegment = (currentSegment % maxNumSegments);
      }
      return currentSegment;
    }

    /**
     * Read domain-specific limits into a map
     * 
     * @param conf
     * @param partition
     *          partition ID of current task: domains not matching the partition
     *          ID are skipped to keep the map small
     * @return map <domain, limits>
     */
    public static Map<String, DomainLimits> readLimitsFile(Configuration conf) {
      return readLimitsFile(conf, -1, 1, null);
    }

    /**
     * Read domain-specific limits into a map
     * 
     * @param conf
     * @param partition
     *          partition ID of current task: domains not matching the partition
     *          ID are skipped to keep the map small
     * @param numReduces
     *          number of reduce tasks determining number of partitions (fetch
     *          lists)
     * @param selector
     *          Selector to determine partition ID for a given domain
     * @return map <domain, limits>
     */
    private static Map<String, DomainLimits> readLimitsFile(Configuration conf,
        int partition, int numReduces, Selector selector) {
      String limitsFile = conf.get(GENERATOR_DOMAIN_LIMITS_FILE);
      if (limitsFile != null) {
        LOG.info("Reading domain-specific limits from {}", limitsFile);
        try (Reader limitsReader = conf.getConfResourceAsReader(limitsFile)) {
          return readLimitsFile(limitsReader, partition, numReduces, selector);
        } catch (IOException e) {
          LOG.error("Failed to read domain-specific limits", e);
        }
      }
      return null;
    }

    private static Map<String, DomainLimits> readLimitsFile(Reader limitsReader,
        int partition, int numReduces, Selector selector) throws IOException {

      Map<String, DomainLimits> domainLimits = new HashMap<>();

      BufferedReader reader = new BufferedReader(limitsReader);

      String line = null;
      String[] splits = null;
      int skipped = 0;

      // Read all lines
      while ((line = reader.readLine()) != null) {
        // Skip blank lines and comments
        if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
          // Split the line by TAB
          splits = line.split("\t");

          if (splits.length < 5) {
            LOG.warn("Invalid line: {}", line);
            continue;
          }
          int urls, urlsHost, hosts, parts;
          try {
            urls = Integer.parseInt(splits[1]);
            urlsHost = Integer.parseInt(splits[2]);
            hosts = Integer.parseInt(splits[3]);
            parts = Integer.parseInt(splits[4]);
          } catch (NumberFormatException e) {
            LOG.warn("Invalid number in line: {} - {}", line, e.getMessage());
            continue;
          }
          String domain = splits[0].toLowerCase(Locale.ROOT);
          if (partition >= 0) {
            DomainScorePair key = new DomainScorePair();
            key.set(domain, .0f);
            int p = selector.getPartition(key, null, numReduces);
            if (p != partition) {
              skipped++;
              continue;
            }
          }
          domainLimits.put(domain,
              new DomainLimits(urls, urlsHost, hosts, parts));
        }
      }
      LOG.info("Loaded domain limits for {} domains", domainLimits.size());
      if (skipped > 0) {
        LOG.info(" - skipped {} domains because of unmatched partition ID {}",
            skipped, partition);
      }
      return domainLimits;
    }

    /*
     * Limit the number of URLs per host/domain and assign segment number to
     * every record.
     */
    public void reduce(DomainScorePair key, Iterable<SelectorEntry> values,
        Context context) throws IOException, InterruptedException {

      int hostOrDomainCount = 0;
      int segment = nextSegment();

      Map<String, int[]> hosts = null;
      int[] segments = null;
      int maxCountPerSegment = maxCount;
      int maxCountPerHostTotal = -1;
      if (maxCountPerHost > 0) {
        maxCountPerHostTotal = maxCountPerHost * maxNumSegments;
      }
      int maxHosts = maxHostsPerDomain;
      int maxHostsOverflowCount = 0;
      String domain = null;
      if (byDomainWithHostLimits) {
        hosts = new HashMap<>();
        segments = new int[maxNumSegments];
        domain = key.getDomain().toString();
        int p = selector.getPartition(key, null, numReduces);
        if (p != partition) {
          LOG.error(
              "Partition for domain {} not equal task partition: {} <> {}",
              domain, p, partition);
        }
        if (domainLimits != null) {
          DomainLimits limits = domainLimits.get(key.domain.toString());
          if (limits != null) {
            LOG.info("Domain-specific limits for {}: {}", key.domain, limits);
            maxCountPerSegment = limits.maxURLs;
            maxCountPerHostTotal = limits.maxURLsPerHost * maxNumSegments;
            maxHosts = limits.maxHosts;
          }
        }
      }
      int maxCountTotal = -1;
      if (maxCountPerSegment > 0) {
        maxCountTotal = maxCountPerSegment * maxNumSegments;
      }

      for (SelectorEntry entry : values) {

        hostOrDomainCount++;

        if (maxCountTotal > 0 && hostOrDomainCount > maxCountTotal) {
          LOG.info(
              "Host or domain {} has more than {} URLs for all {} segments. Additional URLs won't be included in the fetchlist.",
              key.getDomain(), maxCountTotal, maxNumSegments);
          context.getCounter("Generator", "SKIPPED_DOMAINS_OVERFLOW")
              .increment(1);
          break;
        }

        if (byDomainWithHostLimits) {
          String host = null;
          try {
            host = new URL(entry.url.toString()).getHost().toLowerCase(Locale.ROOT);
            if (host.endsWith(domain)) {
              // clip common domain name suffix to save storage space in map keys
              host = host.substring(0, host.length()-domain.length());
            } else {
              LOG.warn("Host {} does not have domain {} as suffix!", host, domain);
            }
          } catch (MalformedURLException e) {
            hostOrDomainCount--;
            continue;
          }
          int[] counts = hosts.get(host);
          if (counts == null) {
            if (maxHosts > 0 && hosts.size() >= maxHosts) {
              // skip current host, max. number of unique hosts reached for domain
              hostOrDomainCount--;
              maxHostsOverflowCount++;
              continue;
            }
            counts = new int[3];
            counts[0] = 0;
            counts[1] = nextSegment();
            counts[2] = 0;
            hosts.put(host, counts);
          } else if (maxCountPerHostTotal > 0 && counts[0] >= maxCountPerHostTotal) {
            hostOrDomainCount--;
            if (counts[0] == maxCountPerHostTotal) {
              LOG.info(
                  "Host {}{} (domain: {}) has more than {} URLs for all {} segments. Additional URLs won't be included in the fetchlist.",
                  host, domain, domain, maxCountPerHostTotal, maxNumSegments);
              context.getCounter("Generator", "SKIPPED_HOSTS_NUM_URLS_OVERFLOW")
                .increment(1);
            }
            context.getCounter("Generator", "SKIPPED_URLS_HOST_OVERFLOW")
              .increment(1);
            counts[0]++;
            continue;
          } else if ((counts[2] % keepMinUrlsPerSegment) == 0) {
            counts[1] = nextSegment();
            counts[2] = 0;
          }
          segment = counts[1];
          // select next segment if there are already too many URLs from the current domain
          if (maxNumSegments > 1 && segments[segment] >= maxCountPerSegment) {
            for (int i = 1; i < maxNumSegments; i++) {
              int s = (segment + i) % maxNumSegments;
              if (segments[s] < maxCountPerSegment) {
                counts[1] = segment = s;
                counts[2] = 0;
                break;
              }
            }
          }
          counts[0]++;
          counts[2]++;
          segments[segment]++;
          entry.segnum.set(segment);
        } else {
          entry.segnum.set(segment);
          if ((hostOrDomainCount % keepMinUrlsPerSegment) == 0) {
            segment = nextSegment();
          }
        }

        context.getCounter("Selected by status",
            CrawlDatum.getStatusName(entry.datum.getStatus())).increment(1);

        context.write(key.getScore(), entry);
      }

      if (maxHostsOverflowCount > 0) {
        context.getCounter("Generator", "SKIPPED_DOMAINS_NUM_HOSTS_OVERFLOW")
            .increment(1);
        context.getCounter("Generator", "SKIPPED_URLS_NUM_HOSTS_OVERFLOW")
          .increment(maxHostsOverflowCount);
        LOG.info(
            "Domain {} has more than {} hosts, skipped {} URLs from remaining hosts",
            key.getDomain(), maxHosts, maxHostsOverflowCount);
      }
    }

  }

  public static class SegmenterKey implements WritableComparable<SegmenterKey> {
    private Text url = new Text();
    private IntWritable segment = new IntWritable();

    public void set(String url, int segment) {
      this.url.set(url);
      this.segment.set(segment);
    }

    public void set(Text url, IntWritable segment) {
      this.url = url;
      this.segment = segment;
    }

    public Text getUrl() {
      return url;
    }

    public IntWritable getSegment() {
      return segment;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      url.readFields(in);
      segment.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      url.write(out);
      segment.write(out);
    }

    @Override
    public int hashCode() {
      return url.hashCode() + segment.hashCode();
    }

    @Override
    public boolean equals(Object right) {
      if (right instanceof SegmenterKey) {
        SegmenterKey r = (SegmenterKey) right;
        return r.url.equals(url) && r.segment.equals(segment);
      } else {
        return false;
      }
    }

    /* Sorts primary by segment, secondary by URL */
    @Override
    public int compareTo(SegmenterKey o) {
      if (!segment.equals(o.getSegment())) {
        return segment.compareTo(o.getSegment());
      } else if (!url.equals(o.getUrl())) {
        return url.compareTo(o.getUrl());
      } else {
        return 0;
      }
    }

  }

  public static class UrlHashComparator extends WritableComparator {

    HashComparator comp = new HashComparator();

    public UrlHashComparator() {
      super(SegmenterKey.class, true);
    }

    public int compare(SegmenterKey a, SegmenterKey b) {
      return comp.compare(a.getUrl(), b.getUrl());
    }

    @Override
    public int compare(Object a, Object b) {
      return compare((SegmenterKey) a, (SegmenterKey) b);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      return compare((SegmenterKey) a, (SegmenterKey) b);
    }
  }

  public static class SegmentComparator extends WritableComparator {
    public SegmentComparator() {
      super(SegmenterKey.class, true);
    }

    public int compare(SegmenterKey a, SegmenterKey b) {
      return a.getSegment().compareTo(b.getSegment());
    }

    @Override
    public int compare(Object a, Object b) {
      return compare((SegmenterKey) a, (SegmenterKey) b);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      return compare((SegmenterKey) a, (SegmenterKey) b);
    }
  }

  public static class SegmentPartitioner
      extends Partitioner<SegmenterKey, Writable> implements Configurable {

    private Configuration conf;

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public int getPartition(SegmenterKey key, Writable value,
        int numReduceTasks) {
      return key.segment.get() % numReduceTasks;
    }

  }

  public static class SegmenterMapper extends
      Mapper<FloatWritable, SelectorEntry, SegmenterKey, SelectorEntry> {

    SegmenterKey outputKey = new SegmenterKey();

    public void map(FloatWritable key, SelectorEntry value, Context context)
        throws IOException, InterruptedException {
      outputKey.set(value.url, value.segnum);
      context.write(outputKey, value);
    }

  }

  /*
   * This takes the filtered records from the Selector job, limits the number of
   * records per segment in the reducer and saves each segment to its own file.
   */
  public static class SegmenterReducer
      extends Reducer<SegmenterKey, SelectorEntry, Text, SelectorEntry> {

    private long maxPerSegment;
    private MultipleOutputs<Text, SelectorEntry> mos;

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      maxPerSegment = conf.getLong(GENERATOR_TOP_N, Long.MAX_VALUE);
      mos = new MultipleOutputs<Text, SelectorEntry>(context);
    }

    public void reduce(SegmenterKey key, Iterable<SelectorEntry> values,
        Context context) throws IOException, InterruptedException {
      long count = 0;
      int segnum = -1;
      String fileName = null;
      for (SelectorEntry entry : values) {
        if (segnum == -1) {
          segnum = entry.segnum.get(); // same as key.getSegment().get()
          fileName = generateFileName(entry);
          LOG.info("Writing segment {} to {}", segnum, fileName);
        }
        if (count < maxPerSegment) {
          mos.write("sequenceFiles", entry.url, entry, fileName);
        } else {
          context.getCounter("Generator", "SKIPPED_RECORDS_SEGMENT_OVERFLOW")
              .increment(1);
          if (count == maxPerSegment) {
            LOG.info(
                "Maximum number of URLs per segment reached for segment {}, skipping remaining URLs",
                key.getSegment().get());
          }
        }
        count++;
      }
    }

    private String generateFileName(SelectorEntry entry) {
      return "fetchlist-" + entry.segnum.toString() + "/part";
    }

    @Override
    public void cleanup(Context context)
        throws IOException, InterruptedException {
      mos.close();
    }

  }

  public static class SelectorInverseMapper
      extends Mapper<Text, SelectorEntry, Text, CrawlDatum> {

    private int numLists = 1;
    private int mapno = 0;
    private long currentTime = System.currentTimeMillis();
    private MultipleOutputs<Text, CrawlDatum> out;
    URLPartitioner partitioner = new URLPartitioner();

    @Override
    public void setup(Context context) {
      out = new MultipleOutputs<Text, CrawlDatum>(context);
      Configuration conf = context.getConfiguration();
      mapno = conf.getInt(Context.TASK_PARTITION,
          random.nextInt(Integer.MAX_VALUE));
      numLists = conf.getInt("num.lists", 1);
      partitioner.setConf(conf);
      partitioner.setDomainLimits(SelectorReducer.readLimitsFile(conf));
    }

    public void map(Text key, SelectorEntry value, Context context)
        throws IOException, InterruptedException {
      out.write("sequenceFilesPartitions", key, value.datum,
          generateFileName(key, value.datum, numLists));
    }

    private String generateFileName(Text key, CrawlDatum value, int numLists) {
      int partition = partitioner.getPartition(key, value, numLists);
      return "" + currentTime + "." + mapno + "/" + CrawlDatum.GENERATE_DIR_NAME
          + "/subfetchlist-" + partition;
    }

    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
      out.close();
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
      // by hashcode correlates less with by-host ordering.
      for (int i = length - 1; i >= 0; i--)
        hash = (31 * hash) + (int) bytes[start + i];
      return hash;
    }
  }

  public Generator2() {
  }

  public Generator2(Configuration conf) {
    setConf(conf);
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
   * @throws Exception
   */
  public Path[] generate(Path dbDir, String dbVersion, Path segments,
      int numLists, long topN, long curTime, boolean filter, boolean norm,
      boolean force, int maxNumSegments, boolean keep, String stage2, String stage1)
      throws Exception {

    Path tempDir = new Path(getConf().get("mapreduce.cluster.temp.dir", ".")
        + "/generate-temp-" + System.currentTimeMillis());

    Path lock = new Path(dbDir, CrawlDb.LOCK_NAME);
    FileSystem fs = lock.getFileSystem(getConf());
    FileSystem tempFs = tempDir.getFileSystem(getConf());
    Path stage1Dir = null, stage2Dir = null;

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("Generator: starting at {}", sdf.format(start));
    LOG.info("Generator: Selecting best-scoring urls due for fetch.");
    LOG.info("Generator: filtering: {}", filter);
    LOG.info("Generator: normalizing: {}", norm);
    if (topN != Long.MAX_VALUE) {
      LOG.info("Generator: perSegment: {}", topN);
    }
    LOG.info("Generator: generate.count.mode = {}", getConf().get(GENERATOR_COUNT_MODE));
    LOG.info("Generator: partition.url.mode = {}", getConf().get(URLPartitioner.PARTITION_MODE_KEY));

    if (stage2 == null && stage1 == null) {
      // map to inverted subset due for fetch, sort by score
      Job job = NutchJob.getInstance(getConf());
      job.setJobName("generate: select from " + dbDir);

      Configuration conf = job.getConfiguration();
      if (numLists == -1) {
        // for politeness create a single partition per fetcher map task
        numLists = Integer.parseInt(conf.get("mapreduce.job.maps"));
      }
      if ("local".equals(conf.get("mapreduce.framework.name"))
          && numLists != 1) {
        // override
        LOG.info(
            "Generator: running in local mode, generating exactly one partition.");
        numLists = 1;
      }
      conf.setLong(GENERATOR_CUR_TIME, curTime);
      // record real generation time
      long generateTime = System.currentTimeMillis();
      conf.setLong(Nutch.GENERATE_TIME_KEY, generateTime);
      conf.setBoolean(GENERATOR_FILTER, filter);
      conf.setBoolean(GENERATOR_NORMALISE, norm);
      conf.setInt(GENERATOR_MAX_NUM_SEGMENTS, maxNumSegments);
      conf.setInt("partition.url.seed", new Random().nextInt());
      job.setSpeculativeExecution(true);
      job.setReduceSpeculativeExecution(true);

      FileInputFormat.addInputPath(job, new Path(dbDir, dbVersion));
      job.setInputFormatClass(SequenceFileInputFormat.class);

      job.setMapperClass(SelectorMapper.class);
      job.setPartitionerClass(Selector.class);
      job.setReducerClass(SelectorReducer.class);
      job.setJarByClass(Selector.class);

      stage1Dir = tempDir.suffix("/stage1");
      FileOutputFormat.setOutputPath(job, stage1Dir);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      job.setMapOutputKeyClass(DomainScorePair.class);
      job.setOutputKeyClass(FloatWritable.class);
      job.setSortComparatorClass(ScoreComparator.class);
      job.setGroupingComparatorClass(DomainComparator.class);
      job.setOutputValueClass(SelectorEntry.class);

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

      long selected = job.getCounters()
          .findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
      if (selected == 0) {
        LOG.warn("Generator: 0 records selected for fetching, exiting ...");
        NutchJob.cleanupAfterFailure(tempDir, lock, fs);
        return null;
      }
    } else if (stage1 != null) {
      stage1Dir = new Path(stage1);
    }

    if (stage2 == null) {
      // Read through the generated URL list and output individual segment files
      Job job = NutchJob.getInstance(getConf());
      job.setJobName("generate: segmenter");
      Configuration conf = job.getConfiguration();
      conf.setLong(GENERATOR_TOP_N, topN);

      job.setSpeculativeExecution(true);
      job.setReduceSpeculativeExecution(true);

      FileInputFormat.addInputPath(job, stage1Dir);
      job.setInputFormatClass(SequenceFileInputFormat.class);

      job.setMapperClass(SegmenterMapper.class);
      job.setReducerClass(SegmenterReducer.class);
      job.setJarByClass(SegmenterMapper.class);

      /*
       * Every reduce partition contains all data from a single segment in a
       * single group to check for the max. segment size.
       */
      job.setGroupingComparatorClass(SegmentComparator.class);
      job.setPartitionerClass(SegmentPartitioner.class);
      /*
       * URLs are shuffled (sorted by pseudo-random hash value).
       */
      job.setSortComparatorClass(UrlHashComparator.class);
      /* ensure that every segments gets its own partition */
      job.setNumReduceTasks(maxNumSegments);

      job.setMapOutputKeyClass(SegmenterKey.class);
      job.setMapOutputValueClass(SelectorEntry.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(SelectorEntry.class);

      stage2Dir = tempDir.suffix("/stage2");
      FileOutputFormat.setOutputPath(job, stage2Dir);
      MultipleOutputs.addNamedOutput(job, "sequenceFiles",
          SequenceFileOutputFormat.class, Text.class, SelectorEntry.class);
      LazyOutputFormat.setOutputFormatClass(job,
          SequenceFileOutputFormat.class);
      
      try {
        boolean success = job.waitForCompletion(true);
        if (!success) {
          String message = "Generator job did not succeed, job status:"
              + job.getStatus().getState() + ", reason: "
              + job.getStatus().getFailureInfo();
          LOG.error(message);
          if (!keep) {
            NutchJob.cleanupAfterFailure(tempDir, lock, fs);
          }
          throw new RuntimeException(message);
        }
      } catch (IOException | InterruptedException | ClassNotFoundException e) {
        LOG.error("Generator job failed: {}", e.getMessage());
        if (!keep) {
          NutchJob.cleanupAfterFailure(tempDir, lock, fs);
        }
        throw e;
      }
    } else {
      stage2Dir = new Path(stage2);
    }
    // read the subdirectories generated in the temporary
    // output and turn them into segments, shuffle URLs
    // to spread URLs of the same host/domain over the entire
    // fetch list
    List<Path> generatedSegments;

    try {
      FileStatus[] status = tempFs.listStatus(stage2Dir);
      List<Path> inputDirs = new ArrayList<Path>();
      for (FileStatus stat : status) {
        Path subfetchlist = stat.getPath();
        if (!subfetchlist.getName().startsWith("fetchlist-"))
          continue;
        inputDirs.add(subfetchlist);
      }
      generatedSegments = partitionSegments(segments.getFileSystem(getConf()),
          segments, inputDirs, numLists);
    } catch (Exception e) {
      LOG.warn("Generator: exception while partitioning segments, exiting ...",
          e);
      if (!keep) {
        tempFs.delete(tempDir, true);
      }
      return null;
    }

    if (!keep) {
      tempFs.delete(tempDir, true);
    }

    long end = System.currentTimeMillis();
    LOG.info("Generator: finished at {}, elapsed: {}", sdf.format(end),
        TimingUtil.elapsedTime(start, end));

    Path[] patharray = new Path[generatedSegments.size()];
    return generatedSegments.toArray(patharray);
  }

  /**
   * Partition segments: one partition is the fetch lists of a single fetcher
   * task.
   */
  private List<Path> partitionSegments(FileSystem fs, Path segmentsDir,
      List<Path> inputDirs, int numLists) throws Exception {
    if (LOG.isInfoEnabled()) {
      LOG.info("Generator: Partitioning selected urls for politeness.");
    }

    List<Path> generatedSegments = new ArrayList<Path>();

    LOG.info("Generator: partitionSegment: {}", segmentsDir);

    Job job = NutchJob.getInstance(getConf());
    job.setJobName("generate: partition " + segmentsDir);

    Configuration conf = job.getConfiguration();
    conf.setInt("partition.url.seed", new Random().nextInt());

    for (Path p : inputDirs) {
      FileInputFormat.addInputPath(job, p);
    }
    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setSpeculativeExecution(false);
    job.setMapSpeculativeExecution(false);
    job.setMapperClass(SelectorInverseMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(CrawlDatum.class);
    job.setJarByClass(SelectorInverseMapper.class);

    /* Ensure output files from step 2 are not split. */
    conf.setLong("mapreduce.input.fileinputformat.split.minsize",
        Long.MAX_VALUE);
    /*
     * Reduce the replication factor to limit the number of open HDFS
     * files/blocks - we may write 100 segments each with 100 partitions /
     * fetchers.
     */
    conf.set("dfs.replication", "1");

    /* Set number of fetchers */
    conf.setInt("num.lists", numLists);
    job.setNumReduceTasks(0);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);
    FileOutputFormat.setOutputPath(job, segmentsDir);
    MultipleOutputs.addNamedOutput(job, "sequenceFilesPartitions",
        SequenceFileOutputFormat.class, Text.class, CrawlDatum.class);
    LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
    if (LOG.isDebugEnabled()) {
      // (for debugging) count records per output file
      // NOTE: may create too many counters if used in production with many
      // segments and partitions
      MultipleOutputs.setCountersEnabled(job, true);
    }

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
      LOG.error("Generator job failed: {}", e.getMessage());
      throw e;
    }

    return generatedSegments;
  }

  /**
   * Generate a fetchlist from the crawldb.
   */
  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new Generator2(),
        args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println(
          "Usage: Generator2 <crawldb> <segments_dir> [-force] [-keep] [-numPerSegment N] [-numFetchers numFetchers] [-adddays numDays] [-noFilter] [-noNorm] [-maxNumSegments num]");
      return -1;
    }

    Path dbDir = new Path(args[0]);
    Path segmentsDir = new Path(args[1]);
    long curTime = System.currentTimeMillis();
    long topN = Long.MAX_VALUE;
    int numFetchers = -1;
    boolean filter = true;
    boolean norm = true;
    boolean force = false;
    boolean keep = false;
    int maxNumSegments = 1;
    String stage2 = null;
    String stage1 = null;
    String dbVersion = CrawlDb.CURRENT_NAME;

    for (int i = 2; i < args.length; i++) {
      if ("-numPerSegment".equals(args[i])) {
        topN = Long.parseLong(args[i + 1]);
        i++;
      } else if ("-numFetchers".equals(args[i])) {
        numFetchers = Integer.parseInt(args[i + 1]);
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
      } else if ("-keep".equals(args[i])) {
        keep = true;
      } else if ("-stage1".equals(args[i])) {
        stage1 = args[++i];
      } else if ("-stage2".equals(args[i])) {
        stage2 = args[++i];
      } else if ("-dbVersion".equals(args[i])) {
        dbVersion = args[++i];
      }
    }

    try {
      Path[] segs = generate(dbDir, dbVersion, segmentsDir, numFetchers, topN,
          curTime, filter, norm, force, maxNumSegments, keep, stage2, stage1);
      if (segs == null)
        return -1;
    } catch (Exception e) {
      LOG.error("Generator failed with", e);
      return -1;
    }
    return 0;
  }

}

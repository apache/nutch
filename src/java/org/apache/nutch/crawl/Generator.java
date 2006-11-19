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
import java.net.*;
import java.util.*;
import java.text.*;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolBase;
import org.apache.hadoop.fs.Path;

import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/** Generates a subset of a crawl db to fetch. */
public class Generator extends ToolBase {

  public static final String CRAWL_GENERATE_FILTER = "crawl.generate.filter";
  public static final String GENERATE_MAX_PER_HOST_BY_IP = "generate.max.per.host.by.ip";
  public static final String GENERATE_MAX_PER_HOST = "generate.max.per.host";
  public static final String CRAWL_TOP_N = "crawl.topN";
  public static final String CRAWL_GEN_CUR_TIME = "crawl.gen.curTime";
  public static final Log LOG = LogFactory.getLog(Generator.class);
  
  public static class SelectorEntry implements Writable {
    public Text url;
    public CrawlDatum datum;
    
    public SelectorEntry() {
      url = new Text();
      datum = new CrawlDatum();
    }

    public void readFields(DataInput in) throws IOException {
      url.readFields(in);
      datum.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
      url.write(out);
      datum.write(out);
    }
    
    public String toString() {
      return "url=" + url.toString() + ", datum=" + datum.toString();
    }
  }

  /** Selects entries due for fetch. */
  public static class Selector implements Mapper, Partitioner, Reducer {
    private long curTime;
    private long limit;
    private long count;
    private HashMap hostCounts = new HashMap();
    private int maxPerHost;
    private Partitioner hostPartitioner = new PartitionUrlByHost();
    private URLFilters filters;
    private URLNormalizers normalizers;
    private ScoringFilters scfilters;
    private SelectorEntry entry = new SelectorEntry();
    private FloatWritable sortValue = new FloatWritable();
    private boolean byIP;
    private long dnsFailure = 0L;
    private boolean filter;

    public void configure(JobConf job) {
      curTime = job.getLong(CRAWL_GEN_CUR_TIME, System.currentTimeMillis());
      limit = job.getLong(CRAWL_TOP_N,Long.MAX_VALUE)/job.getNumReduceTasks();
      maxPerHost = job.getInt(GENERATE_MAX_PER_HOST, -1);
      byIP = job.getBoolean(GENERATE_MAX_PER_HOST_BY_IP, false);
      filters = new URLFilters(job);
      normalizers = new URLNormalizers(job, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
      scfilters = new ScoringFilters(job);
      hostPartitioner.configure(job);
      filter = job.getBoolean(CRAWL_GENERATE_FILTER, true);
    }

    public void close() {}

    /** Select & invert subset due for fetch. */
    public void map(WritableComparable key, Writable value,
                    OutputCollector output, Reporter reporter)
      throws IOException {
      Text url = (Text)key;
      if (filter) {
        // If filtering is on don't generate URLs that don't pass URLFilters
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
      CrawlDatum crawlDatum = (CrawlDatum)value;

      if (crawlDatum.getStatus() == CrawlDatum.STATUS_DB_GONE)
        return;                                   // don't retry

      if (crawlDatum.getFetchTime() > curTime)
        return;                                   // not time yet

      float sort = 1.0f;
      try {
        sort = scfilters.generatorSortValue((Text)key, crawlDatum, sort);
      } catch (ScoringFilterException sfe) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Couldn't filter generatorSortValue for " + key + ": " + sfe);
        }
      }
      // sort by decreasing score, using DecreasingFloatComparator
      sortValue.set(sort);
      entry.datum = crawlDatum;
      entry.url = (Text)key;
      output.collect(sortValue, entry);          // invert for sort by score
    }

    /** Partition by host. */
    public int getPartition(WritableComparable key, Writable value,
                            int numReduceTasks) {
      return hostPartitioner.getPartition(((SelectorEntry)value).url, key,
                                          numReduceTasks);
    }

    /** Collect until limit is reached. */
    public void reduce(WritableComparable key, Iterator values,
                       OutputCollector output, Reporter reporter)
      throws IOException {

      while (values.hasNext() && count < limit) {

        SelectorEntry entry = (SelectorEntry)values.next();
        Text url = entry.url;

        if (maxPerHost > 0) {                     // are we counting hosts?
          URL u = new URL(url.toString());
          String host = u.getHost();
          if (host == null) {
            // unknown host, skip
            continue;
          }
          host = host.toLowerCase();
          if (byIP) {
            try {
              InetAddress ia = InetAddress.getByName(host);
              host = ia.getHostAddress();
            } catch (UnknownHostException uhe) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("DNS lookup failed: " + host + ", skipping.");
              }
              dnsFailure++;
              if ((dnsFailure % 1000 == 0) && (LOG.isWarnEnabled())) {
                LOG.warn("DNS failures: " + dnsFailure);
              }
              continue;
            }
          }
          u = new URL(u.getProtocol(), host, u.getPort(), u.getFile());
          String urlString = u.toString();
          try {
            urlString = normalizers.normalize(urlString, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
            host = new URL(urlString).getHost();
          } catch (Exception e) {
            LOG.warn("Malformed URL: '" + urlString + "', skipping (" +
                StringUtils.stringifyException(e) + ")");
            continue;
          }
          IntWritable hostCount = (IntWritable)hostCounts.get(host);
          if (hostCount == null) {
            hostCount = new IntWritable();
            hostCounts.put(host, hostCount);
          }

          // increment hostCount
          hostCount.set(hostCount.get() + 1);

          // skip URL if above the limit per host.
          if (hostCount.get() > maxPerHost) {
            if (hostCount.get() == maxPerHost + 1) {
              if (LOG.isInfoEnabled()) {
                LOG.info("Host " + host + " has more than " + maxPerHost +
                         " URLs." + " Skipping additional.");
              }
            }
            continue;
          }
        }

        output.collect(key, entry);

        // Count is incremented only when we keep the URL
        // maxPerHost may cause us to skip it.
        count++;
      }

    }

  }

  public static class DecreasingFloatComparator extends WritableComparator {

    public DecreasingFloatComparator() {
      super(FloatWritable.class);
    }

    /** Compares two FloatWritables decreasing. */
    public int compare(WritableComparable o1, WritableComparable o2) {
      float thisValue = ((FloatWritable) o1).get();
      float thatValue = ((FloatWritable) o2).get();
      return (thisValue<thatValue ? 1 : (thisValue == thatValue ? 0 : -1));
    }
  }
  
  public static class SelectorInverseMapper extends MapReduceBase implements Mapper {

    public void map(WritableComparable key, Writable value, OutputCollector output, Reporter reporter) throws IOException {
      SelectorEntry entry = (SelectorEntry)value;
      output.collect(entry.url, entry.datum);
    }    
  }
  
  /** Sort fetch lists by hash of URL. */
  public static class HashComparator extends WritableComparator {
    public HashComparator() { super(Text.class); }

    public int compare(WritableComparable a, WritableComparable b) {
      Text url1 = (Text)a;
      Text url2 = (Text)b;
      int hash1 = hash(url1.getBytes(), 0, url1.getLength());
      int hash2 = hash(url2.getBytes(), 0, url2.getLength());
      if (hash1 != hash2) {
        return hash1 - hash2;
      }
      return compareBytes(url1.getBytes(), 0, url1.getLength(),
                          url2.getBytes(), 0, url2.getLength());
    }


    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int hash1 = hash(b1, s1, l1);
      int hash2 = hash(b2, s2, l2);
      if (hash1 != hash2) {
        return hash1 - hash2;
      }
      return compareBytes(b1, s1, l1, b2, s2, l2);
    }

    private static int hash(byte[] bytes, int start, int length) {
      int hash = 1;
      // make later bytes more significant in hash code, so that sorting by
      // hashcode correlates less with by-host ordering.
      for (int i = length-1; i >= 0; i--)
        hash = (31 * hash) + (int)bytes[start+i];
      return hash;
    }
  }

  public Generator() {
    
  }
  
  public Generator(Configuration conf) {
    setConf(conf);
  }
  
  /** Generate fetchlists in a segment. */
  public Path generate(Path dbDir, Path segments)
    throws IOException {
    return generate(dbDir, segments, -1, Long.MAX_VALUE, System
        .currentTimeMillis(), true);
  }

  /** Generate fetchlists in a segment. */
  public Path generate(Path dbDir, Path segments,
                       int numLists, long topN, long curTime, boolean filter)
    throws IOException {

    Path tempDir =
      new Path(getConf().get("mapred.temp.dir", ".") +
               "/generate-temp-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    Path segment = new Path(segments, generateSegmentName());
    Path output = new Path(segment, CrawlDatum.GENERATE_DIR_NAME);

    LOG.info("Generator: Selecting best-scoring urls due for fetch.");
    LOG.info("Generator: starting");
    LOG.info("Generator: segment: " + segment);
    LOG.info("Generator: filtering: " + filter);
    if (topN != Long.MAX_VALUE) {
      LOG.info("Generator: topN: " + topN);
    }

    // map to inverted subset due for fetch, sort by link count
    JobConf job = new NutchJob(getConf());
    job.setJobName("generate: select " + segment);

    if (numLists == -1) {                         // for politeness make
      numLists = job.getNumMapTasks();            // a partition per fetch task
    }
    if ("local".equals(job.get("mapred.job.tracker")) && numLists != 1) {
      // override
      LOG.info("Generator: jobtracker is 'local', generating exactly one partition.");
      numLists = 1;
    }
    job.setLong(CRAWL_GEN_CUR_TIME, curTime);
    job.setLong(CRAWL_TOP_N, topN);
    job.setBoolean(CRAWL_GENERATE_FILTER, filter);

    job.setInputPath(new Path(dbDir, CrawlDatum.DB_DIR_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(Selector.class);
    job.setPartitionerClass(Selector.class);
    job.setReducerClass(Selector.class);

    job.setOutputPath(tempDir);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputKeyComparatorClass(DecreasingFloatComparator.class);
    job.setOutputValueClass(SelectorEntry.class);
    JobClient.runJob(job);

    // invert again, paritition by host, sort by url hash
    if (LOG.isInfoEnabled()) {
      LOG.info("Generator: Partitioning selected urls by host, for politeness.");
    }
    job = new NutchJob(getConf());
    job.setJobName("generate: partition " + segment);
    
    job.setInt("partition.url.by.host.seed", new Random().nextInt());

    job.setInputPath(tempDir);
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(SelectorInverseMapper.class);
    job.setPartitionerClass(PartitionUrlByHost.class);
    job.setNumReduceTasks(numLists);

    job.setOutputPath(output);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);
    job.setOutputKeyComparatorClass(HashComparator.class);
    JobClient.runJob(job);

    new JobClient(getConf()).getFs().delete(tempDir);

    if (LOG.isInfoEnabled()) { LOG.info("Generator: done."); }

    return segment;
  }
  
  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

  public static synchronized String generateSegmentName() {
    try {
      Thread.sleep(1000);
    } catch (Throwable t) {};
    return sdf.format
      (new Date(System.currentTimeMillis()));
  }

  /**
   * Generate a fetchlist from the pagedb and linkdb
   */
  public static void main(String args[]) throws Exception {
    int res = new Generator().doMain(NutchConfiguration.create(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("Usage: Generator <crawldb> <segments_dir> [-topN N] [-numFetchers numFetchers] [-adddays numDays] [-noFilter]");
      return -1;
    }

    Path dbDir = new Path(args[0]);
    Path segmentsDir = new Path(args[1]);
    long curTime = System.currentTimeMillis();
    long topN = Long.MAX_VALUE;
    int numFetchers = -1;
    boolean filter = true;

    for (int i = 2; i < args.length; i++) {
      if ("-topN".equals(args[i])) {
        topN = Long.parseLong(args[i+1]);
        i++;
      } else if ("-numFetchers".equals(args[i])) {
        numFetchers = Integer.parseInt(args[i+1]);
        i++;
      } else if ("-adddays".equals(args[i])) {
        long numDays = Integer.parseInt(args[i+1]);
        curTime += numDays * 1000L * 60 * 60 * 24;
      } else if ("-noFilter".equals(args[i])) {
        filter = false;
      }
      
    }

    try {
      generate(dbDir, segmentsDir, numFetchers, topN, curTime, filter);
      return 0;
    } catch (Exception e) {
      LOG.fatal("Generator: " + StringUtils.stringifyException(e));
      return -1;
    }
  }
}

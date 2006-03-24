/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.logging.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/** Generates a subset of a crawl db to fetch. */
public class Generator extends Configured {

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.crawl.Generator");

  /** Selects entries due for fetch. */
  public static class Selector implements Mapper, Partitioner, Reducer {
    private long curTime;
    private long limit;
    private long count;
    private HashMap hostCounts = new HashMap();
    private int maxPerHost;
    private Partitioner hostPartitioner = new PartitionUrlByHost();
    private URLFilters filters;

    public void configure(JobConf job) {
      curTime = job.getLong("crawl.gen.curTime", System.currentTimeMillis());
      limit = job.getLong("crawl.topN",Long.MAX_VALUE)/job.getNumReduceTasks();
      maxPerHost = job.getInt("generate.max.per.host", -1);
      filters = new URLFilters(job);
    }

    public void close() {}

    /** Select & invert subset due for fetch. */
    public void map(WritableComparable key, Writable value,
                    OutputCollector output, Reporter reporter)
      throws IOException {
      UTF8 url = (UTF8)key;
      // don't generate URLs that don't pass URLFilters
      try {
        if (filters.filter(url.toString()) == null)
          return;
      } catch (URLFilterException e) {
        LOG.warning("Couldn't filter url: " + url + " (" + e.getMessage() + ")");
      }
      CrawlDatum crawlDatum = (CrawlDatum)value;

      if (crawlDatum.getStatus() == CrawlDatum.STATUS_DB_GONE)
        return;                                   // don't retry

      if (crawlDatum.getFetchTime() > curTime)
        return;                                   // not time yet

      output.collect(crawlDatum, key);          // invert for sort by score
    }

    /** Partition by host (value). */
    public int getPartition(WritableComparable key, Writable value,
                            int numReduceTasks) {
      return hostPartitioner.getPartition((WritableComparable)value, key,
                                          numReduceTasks);
    }

    /** Collect until limit is reached. */
    public void reduce(WritableComparable key, Iterator values,
                       OutputCollector output, Reporter reporter)
      throws IOException {

      while (values.hasNext() && count < limit) {

        UTF8 url = (UTF8)values.next();

        if (maxPerHost > 0) {                     // are we counting hosts?
          String host = new URL(url.toString()).getHost();
          Integer hostCount = (Integer)hostCounts.get(host);
          if (hostCount != null) {
            if (hostCount.intValue() >= maxPerHost)
              continue;                           // too many from host
            hostCounts.put(host, new Integer(hostCount.intValue()+1));
          } else {                                // update host count
            hostCounts.put(host, new Integer(1));
          }
        }

        output.collect(key, url);

        // Count is incremented only when we keep the URL
        // maxPerHost may cause us to skip it.
        count++;
      }

    }

  }

  /** Sort fetch lists by hash of URL. */
  public static class HashComparator extends WritableComparator {
    public HashComparator() { super(UTF8.class); }

    public int compare(WritableComparable a, WritableComparable b) {
      UTF8 url1 = (UTF8)a;
      UTF8 url2 = (UTF8)b;
      int hash1 = hash(url1.getBytes(), 0, url1.getLength());
      int hash2 = hash(url2.getBytes(), 0, url2.getLength());
      if (hash1 != hash2) {
        return hash1 - hash2;
      }
      return compareBytes(url1.getBytes(), 0, url1.getLength(),
                          url2.getBytes(), 0, url2.getLength());
    }


    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int n1 = readUnsignedShort(b1, s1);
      int n2 = readUnsignedShort(b2, s2);
      int hash1 = hash(b1, s1+2, n1);
      int hash2 = hash(b2, s2+2, n2);
      if (hash1 != hash2) {
        return hash1 - hash2;
      }
      return compareBytes(b1, s1+2, n1, b2, s2+2, n2);
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

  /** Construct a generator. */
  public Generator(Configuration conf) {
    super(conf);
  }

  /** Generate fetchlists in a segment. */
  public File generate(File dbDir, File segments)
    throws IOException {
    return generate(dbDir, segments,
                    -1, Long.MAX_VALUE, System.currentTimeMillis());
  }

  /** Generate fetchlists in a segment. */
  public File generate(File dbDir, File segments,
                       int numLists, long topN, long curTime)
    throws IOException {

    File tempDir =
      new File(getConf().get("mapred.temp.dir", ".") +
               "/generate-temp-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    File segment = new File(segments, getDate());
    File output = new File(segment, CrawlDatum.GENERATE_DIR_NAME);

    LOG.info("Generator: starting");
    LOG.info("Generator: segment: " + segment);

    // map to inverted subset due for fetch, sort by link count
    LOG.info("Generator: Selecting most-linked urls due for fetch.");
    JobConf job = new NutchJob(getConf());
    job.setJobName("generate: select " + segment);
    
    if (numLists == -1) {                         // for politeness make
      numLists = job.getNumMapTasks();            // a partition per fetch task
    }

    job.setLong("crawl.gen.curTime", curTime);
    job.setLong("crawl.topN", topN);

    job.setInputDir(new File(dbDir, CrawlDatum.DB_DIR_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(CrawlDatum.class);

    job.setMapperClass(Selector.class);
    job.setPartitionerClass(Selector.class);
    job.setReducerClass(Selector.class);

    job.setOutputDir(tempDir);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(CrawlDatum.class);
    job.setOutputValueClass(UTF8.class);
    JobClient.runJob(job);

    // invert again, paritition by host, sort by url hash
    LOG.info("Generator: Partitioning selected urls by host, for politeness.");
    job = new NutchJob(getConf());
    job.setJobName("generate: partition " + segment);
    
    job.setInt("partition.url.by.host.seed", new Random().nextInt());

    job.setInputDir(tempDir);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(CrawlDatum.class);
    job.setInputValueClass(UTF8.class);

    job.setMapperClass(InverseMapper.class);
    job.setPartitionerClass(PartitionUrlByHost.class);
    job.setNumReduceTasks(numLists);

    job.setOutputDir(output);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(CrawlDatum.class);
    job.setOutputKeyComparatorClass(HashComparator.class);
    JobClient.runJob(job);

    new JobClient(getConf()).getFs().delete(tempDir);

    LOG.info("Generator: done.");

    return segment;
  }

  private static String getDate() {
    return new SimpleDateFormat("yyyyMMddHHmmss").format
      (new Date(System.currentTimeMillis()));
  }

  /**
   * Generate a fetchlist from the pagedb and linkdb
   */
  public static void main(String args[]) throws Exception {
    if (args.length < 2) {
      System.out.println("Usage: Generator <crawldb> <segments_dir> [-topN N] [-numFetchers numFetchers] [-adddays numDays]");
      return;
    }

    File dbDir = new File(args[0]);
    File segmentsDir = new File(args[1]);
    long curTime = System.currentTimeMillis();
    long topN = Long.MAX_VALUE;
    int numFetchers = -1;

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
      }
    }

    if (topN != Long.MAX_VALUE)
      LOG.info("topN: " + topN);
    Generator gen = new Generator(NutchConfiguration.create());
    gen.generate(dbDir, segmentsDir, numFetchers, topN, curTime);
  }
}

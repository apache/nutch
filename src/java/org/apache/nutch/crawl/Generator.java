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

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;
import org.apache.nutch.mapred.*;
import org.apache.nutch.mapred.lib.*;

/** Generates a subset of a crawl db to fetch. */
public class Generator extends NutchConfigured {

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.crawl.Generator");

  /** Selects entries due for fetch. */
  public static class Selector implements Mapper, Partitioner, Reducer {
    private long curTime;
    private long limit;
    private long count;

    public void configure(JobConf job) {
      curTime = job.getLong("crawl.gen.curTime", System.currentTimeMillis());
      limit = job.getLong("crawl.gen.limit", Long.MAX_VALUE);
    }

    /** Select & invert subset due for fetch. */
    public void map(WritableComparable key, Writable value,
                    OutputCollector output) throws IOException {
      CrawlDatum crawlDatum = (CrawlDatum)value;

      if (crawlDatum.getStatus() == CrawlDatum.STATUS_DB_GONE)
        return;                                   // don't retry

      if (crawlDatum.getFetchTime() > curTime)
        return;                                   // not time yet

      output.collect(crawlDatum, key);          // invert for sort by linkCount
    }

    /** Hash urls to randomize link counts accross partitions. */
    public int getPartition(WritableComparable key, Writable value,
                            int numReduceTasks) {
      return (value.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }

    /** Collect until limit is reached. */
    public void reduce(WritableComparable key, Iterator values,
                       OutputCollector output) throws IOException {
      while (values.hasNext() && ++count < limit) {
        output.collect(key, (Writable)values.next());
      }

    }

  }

  /** Sort fetch lists by hash of URL. */
  public static class HashComparator extends WritableComparator {
    public HashComparator() { super(UTF8.class); }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return hash(b1, s1, l1) - hash(b2, s2, l2);
    }

    private static int hash(byte[] bytes, int start, int length) {
      int hash = 1;
      for (int i = 0; i < length; i++)
        hash = (31 * hash) + (int)bytes[start+i];
      return hash;
    }
  }

  /** Construct a generator. */
  public Generator(NutchConf conf) {
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
      new File("generate-temp-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    File segment = new File(segments, getDate());
    File output = new File(segment, CrawlDatum.GENERATE_DIR_NAME);

    LOG.info("Generator: starting");
    LOG.info("Generator: segment: " + segment);

    // map to inverted subset due for fetch, sort by link count
    LOG.info("Generator: Selecting most-linked urls due for fetch.");
    JobConf job = new JobConf(getConf());
    
    if (numLists == -1) {                         // for politeness make
      numLists = job.getNumMapTasks();            // a partition per fetch task
    }

    job.setLong("crawl.gen.curTime", curTime);
    job.setLong("crawl.gen.limit", topN / job.getNumReduceTasks());

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
    job = new JobConf(getConf());
    
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
    Generator gen = new Generator(NutchConf.get());
    gen.generate(dbDir, segmentsDir, numFetchers, topN, curTime);
  }
}

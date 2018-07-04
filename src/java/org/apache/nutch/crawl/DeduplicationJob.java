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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic deduplicator which groups fetched URLs with the same digest and marks
 * all of them as duplicate except the one with the highest score (based on the
 * score in the crawldb, which is not necessarily the same as the score
 * indexed). If two (or more) documents have the same score, then the document
 * with the latest timestamp is kept. If the documents have the same timestamp
 * then the one with the shortest URL is kept. The documents marked as duplicate
 * can then be deleted with the command CleaningJob.
 ***/
public class DeduplicationJob extends NutchTool implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private final static Text urlKey = new Text("_URLTEMPKEY_");
  private final static String DEDUPLICATION_GROUP_MODE = "deduplication.group.mode";
  private final static String DEDUPLICATION_COMPARE_ORDER = "deduplication.compare.order";

  public static class DBFilter extends
      Mapper<Text, CrawlDatum, BytesWritable, CrawlDatum> {
      
    private String groupMode;

    public void setup(Mapper<Text, CrawlDatum, BytesWritable, CrawlDatum>.Context context) {
      Configuration arg0 = context.getConfiguration();
      groupMode = arg0.get(DEDUPLICATION_GROUP_MODE);
    }

    public void close() throws IOException {
    }

    public void map(Text key, CrawlDatum value,
        Context context)
        throws IOException, InterruptedException {

      if (value.getStatus() == CrawlDatum.STATUS_DB_FETCHED
          || value.getStatus() == CrawlDatum.STATUS_DB_NOTMODIFIED) {
        // || value.getStatus() ==CrawlDatum.STATUS_DB_GONE){
        byte[] signature = value.getSignature();
        if (signature == null)
          return;
        String url = key.toString();
        BytesWritable sig = null;
        byte[] data;
        switch (groupMode) {
          case "none":
            sig = new BytesWritable(signature);
            break;
          case "host":
            byte[] host = URLUtil.getHost(url).getBytes();
            data = new byte[signature.length + host.length];
            System.arraycopy(signature, 0, data, 0, signature.length);
            System.arraycopy(host, 0, data, signature.length, host.length);
            sig = new BytesWritable(data);
            break;
          case "domain":
            byte[] domain = URLUtil.getDomainName(url).getBytes();
            data = new byte[signature.length + domain.length];
            System.arraycopy(signature, 0, data, 0, signature.length);
            System.arraycopy(domain, 0, data, signature.length, domain.length);
            sig = new BytesWritable(data);
            break;
        }
        // add the URL as a temporary MD
        value.getMetaData().put(urlKey, key);
        // reduce on the signature optionall grouped on host or domain or not at all
        context.write(sig, value);
      }
    }
  }

  public static class DedupReducer extends
      Reducer<BytesWritable, CrawlDatum, Text, CrawlDatum> {

    private String[] compareOrder;
    
    public void setup(Reducer<BytesWritable, CrawlDatum, Text, CrawlDatum>.Context context) {
      Configuration arg0 = context.getConfiguration();
      compareOrder = arg0.get(DEDUPLICATION_COMPARE_ORDER).split(",");
    }

    private void writeOutAsDuplicate(CrawlDatum datum,
        Context context)
        throws IOException, InterruptedException {
      datum.setStatus(CrawlDatum.STATUS_DB_DUPLICATE);
      Text key = (Text) datum.getMetaData().remove(urlKey);
      context.getCounter("DeduplicationJobStatus",
          "Documents marked as duplicate").increment(1);
      context.write(key, datum);
    }

    public void reduce(BytesWritable key, Iterable<CrawlDatum> values,
        Context context)
        throws IOException, InterruptedException {
      CrawlDatum existingDoc = null;

      outerloop:
      for (CrawlDatum newDoc : values) {
        if (existingDoc == null) {
          existingDoc = new CrawlDatum();
          existingDoc.set(newDoc);
          continue;
        }

        for (int i = 0; i < compareOrder.length; i++) {
          switch (compareOrder[i]) {
            case "score":
              // compare based on score
              if (existingDoc.getScore() < newDoc.getScore()) {
                writeOutAsDuplicate(existingDoc, context);
                existingDoc = new CrawlDatum();
                existingDoc.set(newDoc);
                continue outerloop;
              } else if (existingDoc.getScore() > newDoc.getScore()) {
                // mark new one as duplicate
                writeOutAsDuplicate(newDoc, context);
                continue outerloop;
              }
              break;
            case "fetchTime":
              // same score? delete the one which is oldest
              if (existingDoc.getFetchTime() > newDoc.getFetchTime()) {
                // mark new one as duplicate
                writeOutAsDuplicate(newDoc, context);
                continue outerloop;
              } else if (existingDoc.getFetchTime() < newDoc.getFetchTime()) {
                // mark existing one as duplicate
                writeOutAsDuplicate(existingDoc, context);
                existingDoc = new CrawlDatum();
                existingDoc.set(newDoc);
                continue outerloop;
              }
              break;
            case "urlLength":
              // same time? keep the one which has the shortest URL
              String urlExisting;
              String urlnewDoc;
              try {
                urlExisting = URLDecoder.decode(existingDoc.getMetaData().get(urlKey).toString(), "UTF8");
                urlnewDoc = URLDecoder.decode(newDoc.getMetaData().get(urlKey).toString(), "UTF8");
              } catch (UnsupportedEncodingException e) {
                LOG.error("Error decoding: " + urlKey);
                throw new IOException("UnsupportedEncodingException for " + urlKey);
              }
              if (urlExisting.length() < urlnewDoc.length()) {
                // mark new one as duplicate
                writeOutAsDuplicate(newDoc, context);
                continue outerloop;
              } else if (urlExisting.length() > urlnewDoc.length()) {
                // mark existing one as duplicate
                writeOutAsDuplicate(existingDoc, context);
                existingDoc = new CrawlDatum();
                existingDoc.set(newDoc);
                continue outerloop;
              }
              break;
          }
        }

      }
    }

    public void close() throws IOException {

    }
  }

  /** Combine multiple new entries for a url. */
  public static class StatusUpdateReducer extends
      Reducer<Text, CrawlDatum, Text, CrawlDatum> {

    public void setup(Reducer<Text, CrawlDatum, Text, CrawlDatum>.Context context) {
    }

    public void close() {
    }

    private CrawlDatum old = new CrawlDatum();
    private CrawlDatum duplicate = new CrawlDatum();

    public void reduce(Text key, Iterable<CrawlDatum> values,
        Context context)
        throws IOException, InterruptedException {
      boolean duplicateSet = false;

      for (CrawlDatum val : values) {
        if (val.getStatus() == CrawlDatum.STATUS_DB_DUPLICATE) {
          duplicate.set(val);
          duplicateSet = true;
        } else {
          old.set(val);
        }
      }

      // keep the duplicate if there is one
      if (duplicateSet) {
        context.write(key, duplicate);
        return;
      }

      // no duplicate? keep old one then
      context.write(key, old);
    }
  }

  public int run(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: DeduplicationJob <crawldb> [-group <none|host|domain>] [-compareOrder <score>,<fetchTime>,<urlLength>]");
      return 1;
    }

    String group = "none";
    Path crawlDb = new Path(args[0]);
    String compareOrder = "score,fetchTime,urlLength";

    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-group")) 
        group = args[++i];
      if (args[i].equals("-compareOrder")) {
        compareOrder = args[++i];

        if (compareOrder.indexOf("score") == -1 ||
            compareOrder.indexOf("fetchTime") == -1 ||
            compareOrder.indexOf("urlLength") == -1) {
          System.err.println("DeduplicationJob: compareOrder must contain score, fetchTime and urlLength.");
          return 1;
        }
      }
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("DeduplicationJob: starting at " + sdf.format(start));

    Path tempDir = new Path(crawlDb, "dedup-temp-"
        + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    Job job = NutchJob.getInstance(getConf());
    Configuration conf = job.getConfiguration();
    job.setJobName("Deduplication on " + crawlDb);
    conf.set(DEDUPLICATION_GROUP_MODE, group);
    conf.set(DEDUPLICATION_COMPARE_ORDER, compareOrder);
    job.setJarByClass(DeduplicationJob.class);

    FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormatClass(SequenceFileInputFormat.class);

    FileOutputFormat.setOutputPath(job, tempDir);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(CrawlDatum.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    job.setMapperClass(DBFilter.class);
    job.setReducerClass(DedupReducer.class);

    FileSystem fs = tempDir.getFileSystem(getConf());
    try {
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "Crawl job did not succeed, job status:"
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        fs.delete(tempDir, true);
        throw new RuntimeException(message);
      }
      CounterGroup g = job.getCounters().getGroup("DeduplicationJobStatus");
      if (g != null) {
        Counter counter = g.findCounter("Documents marked as duplicate");
        long dups = counter.getValue();
        LOG.info("Deduplication: " + (int) dups
            + " documents marked as duplicates");
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("DeduplicationJob: " + StringUtils.stringifyException(e));
      fs.delete(tempDir, true);
      return -1;
    }

    // merge with existing crawl db
    if (LOG.isInfoEnabled()) {
      LOG.info("Deduplication: Updating status of duplicate urls into crawl db.");
    }

    Job mergeJob = CrawlDb.createJob(getConf(), crawlDb);
    FileInputFormat.addInputPath(mergeJob, tempDir);
    mergeJob.setReducerClass(StatusUpdateReducer.class);
    mergeJob.setJarByClass(DeduplicationJob.class);

    fs = crawlDb.getFileSystem(getConf());
    Path outPath = FileOutputFormat.getOutputPath(job);
    Path lock = CrawlDb.lock(getConf(), crawlDb, false);
    try {
      boolean success = mergeJob.waitForCompletion(true);
      if (!success) {
        String message = "Crawl job did not succeed, job status:"
            + mergeJob.getStatus().getState() + ", reason: "
            + mergeJob.getStatus().getFailureInfo();
        LOG.error(message);
        fs.delete(tempDir, true);
        NutchJob.cleanupAfterFailure(outPath, lock, fs);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("DeduplicationMergeJob: " + StringUtils.stringifyException(e));
      fs.delete(tempDir, true);
      NutchJob.cleanupAfterFailure(outPath, lock, fs);
      return -1;
    }

    CrawlDb.install(mergeJob, crawlDb);

    // clean up
    fs.delete(tempDir, true);

    long end = System.currentTimeMillis();
    LOG.info("Deduplication finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(NutchConfiguration.create(),
        new DeduplicationJob(), args);
    System.exit(result);
  }

  @Override
  public Map<String, Object> run(Map<String, Object> args, String crawlId) throws Exception {
    Map<String, Object> results = new HashMap<>();
    String[] arg = new String[1];
    String crawldb;
    if(args.containsKey(Nutch.ARG_CRAWLDB)) {
      crawldb = (String)args.get(Nutch.ARG_CRAWLDB);
    }
    else {
      crawldb = crawlId+"/crawldb";
    }
    arg[0] = crawldb;
    int res = run(arg);
    results.put(Nutch.VAL_RESULT, Integer.toString(res));
    return results;
  }
}

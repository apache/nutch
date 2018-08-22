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
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple deduplication of redirects pointing to the same target. If two
 * redirects point to the same target and
 * <ul>
 * <li>the target is contained in the CrawlDb: both redirects are marked as
 * duplicates</li>
 * <li>the target is not found in the CrawlDb: one redirect is marked as
 * duplicate. Which one is chosen depends on the criteria defined by the
 * <code>-compareOrder</code> argument.</li>
 * </ul>
 *
 * Unlike {@link DeduplicationJob} which deduplicates based on content
 * signatures, deduplication of redirects is not done to clean up the index
 * &ndash; redirects are not indexed resp. are removed from the index when the
 * indexer is called with <code>-deleteGone</code>. Instead, the aim is to mark
 * URLs in the CrawlDb which would cause unnecessary re-fetches when the fetcher
 * is following redirects (http.redirects.max&nbsp;&gt;&nbsp;0). Duplicates can
 * be removed from the CrawlDb by setting db.update.purge.404 to true.
 */
public class DedupRedirectsJob extends DeduplicationJob {

  public static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Test whether CrawlDatum is a redirect.
   *
   * @param datum
   * @return true if datum is a redirect, false otherwise
   */
  public static boolean isRedirect(CrawlDatum datum) {
    byte status = datum.getStatus();
    if (status == CrawlDatum.STATUS_DB_REDIR_PERM
        || status == CrawlDatum.STATUS_DB_REDIR_TEMP) {
      return true;
    }
    if (status == CrawlDatum.STATUS_DB_DUPLICATE) {
      // check for redirects already marked as duplicate
      ProtocolStatus pStatus = getProtocolStatus(datum);
      if (pStatus != null && (pStatus.getCode() == ProtocolStatus.MOVED
          || pStatus.getCode() == ProtocolStatus.TEMP_MOVED)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Return protocol status of CrawlDatum if present.
   * 
   * @param datum
   * @return protocol status or null if not present
   */
  public static ProtocolStatus getProtocolStatus(CrawlDatum datum) {
    if (datum.getMetaData().containsKey(Nutch.WRITABLE_PROTO_STATUS_KEY))
      return (ProtocolStatus) datum.getMetaData()
          .get(Nutch.WRITABLE_PROTO_STATUS_KEY);
    return null;
  }

  /**
   * Get target URL of a redirect. Note: CrawlDatum is assumed to be a redirect.
   *
   * @param datum
   * @return redirect target URL or null if not available or datum is not a
   *         redirect
   */
  public static String getTargetURL(CrawlDatum datum) {
    ProtocolStatus pStatus = getProtocolStatus(datum);
    if (pStatus != null) {
      return pStatus.getMessage();
    }
    return null;
  }

  /**
   * Reset duplicate status of a redirect marked as duplicate and restore old
   * status (permanent or temporary redirect).
   *
   * @param datum
   */
  private static void unsetDuplicateStatus(CrawlDatum datum) {
    byte status = datum.getStatus();
    if (status == CrawlDatum.STATUS_DB_DUPLICATE) {
      ProtocolStatus pStatus = getProtocolStatus(datum);
      if (pStatus != null) {
        int code = pStatus.getCode();
        if (code == ProtocolStatus.MOVED)
          datum.setStatus(CrawlDatum.STATUS_DB_REDIR_PERM);
        else if (code == ProtocolStatus.TEMP_MOVED)
          datum.setStatus(CrawlDatum.STATUS_DB_REDIR_TEMP);
      }
    }
  }

  public static class RedirTargetMapper
      extends Mapper<Text, CrawlDatum, Text, CrawlDatum> {

    @Override
    public void map(Text key, CrawlDatum value, Context context)
        throws IOException, InterruptedException {

      // output independent of status
      context.write(key, value);

      if (isRedirect(value)) {
        String redirTarget = getTargetURL(value);
        if (redirTarget != null) {
          // keep original URL in CrawlDatum's meta data and emit
          // <redirTarget, crawlDatum>
          value.getMetaData().put(urlKey, key);
          Text redirKey = new Text(redirTarget);
          context.getCounter("DeduplicationJobStatus", "Redirects in CrawlDb")
              .increment(1);
          if (redirKey.equals(key)) {
            // exclude self-referential redirects
            context.getCounter("DeduplicationJobStatus",
                "Self-referential redirects in CrawlDb").increment(1);
          } else {
            context.write(redirKey, value);
          }
        }
      }
    }

  }

  public static class DedupRedirectReducer
      extends DeduplicationJob.DedupReducer<Text> {

    @Override
    public void reduce(Text key, Iterable<CrawlDatum> values, Context context)
        throws IOException, InterruptedException {
      CrawlDatum existingDoc = null;
      for (CrawlDatum newDoc : values) {
        if (existingDoc == null) {
          existingDoc = new CrawlDatum();
          existingDoc.set(newDoc);
          continue;
        }
        CrawlDatum duplicate = null;
        if (isRedirect(existingDoc)
            && !newDoc.getMetaData().containsKey(urlKey)) {
          // newDoc is known as redirect target
          writeOutAsDuplicate(existingDoc, context);
          existingDoc.set(newDoc);
        } else if (isRedirect(newDoc)
            && !existingDoc.getMetaData().containsKey(urlKey)) {
          // existingDoc is known as redirect target
          writeOutAsDuplicate(newDoc, context);
        } else {
          // existingDoc and newDoc are redirects and point to the same target
          duplicate = getDuplicate(existingDoc, newDoc);
          if (duplicate == null) {
            // no decision possible in getDuplicate()
            // and both are redirects: dedup newDoc
            duplicate = newDoc;
          }
          writeOutAsDuplicate(duplicate, context);
          if (duplicate == existingDoc) {
            existingDoc.set(newDoc);
          }
        }
      }
      // finally output existingDoc as non-duplicate if
      if (!isRedirect(existingDoc)) {
        // (a) it is not a redirect
        // Text url = (Text) existingDoc.getMetaData().remove(urlKey);
        context.write(key, existingDoc);
      } else {
        Text origURL = (Text) existingDoc.getMetaData().remove(urlKey);
        if (origURL != null) {
          // (b) it is the value passed to the reducer under the target URL and
          // not under the original URL key. It's a redirect and not a
          // duplicate!
          unsetDuplicateStatus(existingDoc);
          context.write(origURL, existingDoc);
          context.getCounter("DeduplicationJobStatus",
              "Redirects kept as non-duplicates").increment(1);
        } else {
          // (c) it is a self-referential redirect
          String targetURL = getTargetURL(existingDoc);
          if (key.toString().equals(targetURL)) {
            context.write(key, existingDoc);
            context
                .getCounter("DeduplicationJobStatus",
                    "Self-referential redirects kept as non-duplicates")
                .increment(1);
          }
          // else: ignore redirects emitted under original URL because they are
          // collected under the target URL
        }
      }
    }
  }

  public int run(String[] args) throws IOException {

    if (args.length < 1) {
      System.err.println(
          "Usage: DedupRedirectsJob <crawldb> [-group <none|host|domain>] [-compareOrder <score>,<fetchTime>,<urlLength>]");
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

        if (compareOrder.indexOf("score") == -1
            || compareOrder.indexOf("fetchTime") == -1
            || compareOrder.indexOf("urlLength") == -1) {
          System.err.println(
              "DedupRedirectsJob: compareOrder must contain score, fetchTime and urlLength.");
          return 1;
        }
      }
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("DedupRedirectsJob: starting at " + sdf.format(start));

    Path tempDir = new Path(crawlDb, "dedup-temp-"
        + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    Job job = NutchJob.getInstance(getConf());
    Configuration conf = job.getConfiguration();
    job.setJobName("Redirect deduplication on " + crawlDb);
    conf.set(DEDUPLICATION_GROUP_MODE, group);
    conf.set(DEDUPLICATION_COMPARE_ORDER, compareOrder);
    job.setJarByClass(DedupRedirectsJob.class);

    FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormatClass(SequenceFileInputFormat.class);

    FileOutputFormat.setOutputPath(job, tempDir);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(CrawlDatum.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    job.setMapperClass(RedirTargetMapper.class);
    job.setReducerClass(DedupRedirectReducer.class);

    FileSystem fs = tempDir.getFileSystem(getConf());
    long numDuplicates = 0;
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
        numDuplicates = counter.getValue();
        LOG.info(
            "Deduplication: " + (int) numDuplicates + " documents marked as duplicates");
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("DeduplicationJob: " + StringUtils.stringifyException(e));
      fs.delete(tempDir, true);
      return -1;
    }

    if (numDuplicates == 0) {
      if (LOG.isInfoEnabled()) {
        LOG.info("No duplicates found, skip writing CrawlDb");
      }

    } else {
      // temporary output is the deduped crawldb but not in proper sorting
      // (sorted by redirect target), use "merge" job to achieve proper sorting
      if (LOG.isInfoEnabled()) {
        LOG.info("Redirect deduplication: writing CrawlDb.");
      }

      Job mergeJob = CrawlDb.createJob(getConf(), crawlDb);
      FileInputFormat.addInputPath(mergeJob, tempDir);
      mergeJob.setReducerClass(StatusUpdateReducer.class);
      mergeJob.setJarByClass(DedupRedirectsJob.class);
      mergeJob.setReducerClass(StatusUpdateReducer.class);

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
        LOG.error("DedupRedirectsJob: " + StringUtils.stringifyException(e));
        fs.delete(tempDir, true);
        NutchJob.cleanupAfterFailure(outPath, lock, fs);
        return -1;
      }

      CrawlDb.install(mergeJob, crawlDb);
    }

    // clean up
    fs.delete(tempDir, true);

    long end = System.currentTimeMillis();
    LOG.info("DedupRedirectsJob finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(NutchConfiguration.create(),
        new DedupRedirectsJob(), args);
    System.exit(result);
  }

}

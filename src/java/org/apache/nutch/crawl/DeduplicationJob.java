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
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic deduplicator which groups fetched URLs with the same digest and marks
 * all of them as duplicate except the one with the highest score (based on the
 * score in the crawldb, which is not necessarily the same as the score
 * indexed). If two (or more) documents have the same score, then the document
 * with the latest timestamp is kept. If the documents have the same timestamp 
 * then the one with the shortest URL is kept. The documents marked as duplicate can then
 * be deleted with the command CleaningJob.
 ***/
public class DeduplicationJob extends Configured implements Tool {

    public static final Logger LOG = LoggerFactory
            .getLogger(DeduplicationJob.class);

    private final static Text urlKey = new Text("_URLTEMPKEY_");

    public static class DBFilter implements
            Mapper<Text, CrawlDatum, BytesWritable, CrawlDatum> {

        @Override
        public void configure(JobConf arg0) {
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public void map(Text key, CrawlDatum value,
                OutputCollector<BytesWritable, CrawlDatum> output,
                Reporter reporter) throws IOException {

            if (value.getStatus() == CrawlDatum.STATUS_DB_FETCHED
                    || value.getStatus() == CrawlDatum.STATUS_DB_NOTMODIFIED) {
                // || value.getStatus() ==CrawlDatum.STATUS_DB_GONE){
                byte[] signature = value.getSignature();
                if (signature == null) return;
                BytesWritable sig = new BytesWritable(signature);
                // add the URL as a temporary MD
                value.getMetaData().put(urlKey, key);
                // reduce on the signature
                output.collect(sig, value);
            }
        }
    }

    public static class DedupReducer implements
            Reducer<BytesWritable, CrawlDatum, Text, CrawlDatum> {

        private void writeOutAsDuplicate(CrawlDatum datum,
                OutputCollector<Text, CrawlDatum> output, Reporter reporter)
                throws IOException {
            datum.setStatus(CrawlDatum.STATUS_DB_DUPLICATE);
            Text key = (Text) datum.getMetaData().remove(urlKey);
            reporter.incrCounter("DeduplicationJobStatus",
                    "Documents marked as duplicate", 1);
            output.collect(key, datum);
        }

        @Override
        public void reduce(BytesWritable key, Iterator<CrawlDatum> values,
                OutputCollector<Text, CrawlDatum> output, Reporter reporter)
                throws IOException {
            CrawlDatum existingDoc = null;

            while (values.hasNext()) {
                if (existingDoc == null) {
                    existingDoc = new CrawlDatum();
                    existingDoc.set(values.next());
                    continue;
                }
                CrawlDatum newDoc = values.next();
                // compare based on score
                if (existingDoc.getScore() < newDoc.getScore()) {
                    writeOutAsDuplicate(existingDoc, output, reporter);
                    existingDoc = new CrawlDatum();
                    existingDoc.set(newDoc);
                    continue;
                } else if (existingDoc.getScore() > newDoc.getScore()) {
                    // mark new one as duplicate
                    writeOutAsDuplicate(newDoc, output, reporter);
                    continue;
                }
                // same score? delete the one which is oldest
                if (existingDoc.getFetchTime() > newDoc.getFetchTime()) {
                    // mark new one as duplicate
                    writeOutAsDuplicate(newDoc, output, reporter);
                    continue;
                } else if (existingDoc.getFetchTime() < newDoc.getFetchTime()) {
                    // mark existing one as duplicate
                    writeOutAsDuplicate(existingDoc, output, reporter);
                    existingDoc = new CrawlDatum();
                    existingDoc.set(newDoc);
                    continue;
                }
                // same time? keep the one which has the shortest URL
                String urlExisting = existingDoc.getMetaData().get(urlKey).toString();
                String urlnewDoc = newDoc.getMetaData().get(urlKey).toString();
                if (urlExisting.length()<urlnewDoc.length()){
                  // mark new one as duplicate
                  writeOutAsDuplicate(newDoc, output, reporter);
                  continue;
                }
                else if (urlExisting.length()>urlnewDoc.length()){
                  // mark existing one as duplicate
                  writeOutAsDuplicate(existingDoc, output, reporter);
                  existingDoc = new CrawlDatum();
                  existingDoc.set(newDoc);
                  continue;
                }
            }
        }

        @Override
        public void configure(JobConf arg0) {
        }

        @Override
        public void close() throws IOException {

        }
    }

    /** Combine multiple new entries for a url. */
    public static class StatusUpdateReducer implements
            Reducer<Text, CrawlDatum, Text, CrawlDatum> {

        public void configure(JobConf job) {
        }

        public void close() {
        }

        private CrawlDatum old = new CrawlDatum();
        private CrawlDatum duplicate = new CrawlDatum();

        public void reduce(Text key, Iterator<CrawlDatum> values,
                OutputCollector<Text, CrawlDatum> output, Reporter reporter)
                throws IOException {
            boolean duplicateSet = false;
            
            while (values.hasNext()) {
                CrawlDatum val = values.next();
                if (val.getStatus() == CrawlDatum.STATUS_DB_DUPLICATE) {
                    duplicate.set(val);
                    duplicateSet = true;
                } else {
                    old.set(val);
                }
            }

            // keep the duplicate if there is one
            if (duplicateSet) {
                output.collect(key, duplicate);
                return;
            }

            // no duplicate? keep old one then
            output.collect(key, old);
        }
    }

    public int run(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: DeduplicationJob <crawldb>");
            return 1;
        }

        String crawldb = args[0];

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        LOG.info("DeduplicationJob: starting at " + sdf.format(start));

        Path tempDir = new Path(getConf().get("mapred.temp.dir", ".")
                + "/dedup-temp-"
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        JobConf job = new NutchJob(getConf());
        
        job.setJobName("Deduplication on "+crawldb);

        FileInputFormat.addInputPath(job, new Path(crawldb,
                CrawlDb.CURRENT_NAME));
        job.setInputFormat(SequenceFileInputFormat.class);

        FileOutputFormat.setOutputPath(job, tempDir);
        job.setOutputFormat(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(CrawlDatum.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CrawlDatum.class);

        job.setMapperClass(DBFilter.class);
        job.setReducerClass(DedupReducer.class);

        try {
            RunningJob rj = JobClient.runJob(job);
            Group g = rj.getCounters().getGroup("DeduplicationJobStatus");
            if (g != null){
              long dups = g.getCounter("Documents marked as duplicate");
              LOG.info("Deduplication: "+(int)dups+" documents marked as duplicates");
            }
        } catch (final Exception e) {
            LOG.error("DeduplicationJob: " + StringUtils.stringifyException(e));
            return -1;
        }
        
        // merge with existing crawl db
        if (LOG.isInfoEnabled()) {
            LOG.info("Deduplication: Updating status of duplicate urls into crawl db.");
        }

        Path dbPath = new Path(crawldb);
        JobConf mergeJob = CrawlDb.createJob(getConf(), dbPath);
        FileInputFormat.addInputPath(mergeJob, tempDir);
        mergeJob.setReducerClass(StatusUpdateReducer.class);

        try {
            JobClient.runJob(mergeJob);
        } catch (final Exception e) {
            LOG.error("DeduplicationMergeJob: "
                    + StringUtils.stringifyException(e));
            return -1;
        }

        CrawlDb.install(mergeJob, dbPath);

        // clean up
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(tempDir, true);
        
        long end = System.currentTimeMillis();
        LOG.info("Deduplication finished at " + sdf.format(end)
                + ", elapsed: " + TimingUtil.elapsedTime(start, end));

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(NutchConfiguration.create(),
                new DeduplicationJob(), args);
        System.exit(result);
    }
}

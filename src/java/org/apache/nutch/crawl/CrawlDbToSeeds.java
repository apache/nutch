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

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/**
 * Export tuples ⟨url, score⟩ from CrawlDb as seeds to be consumed by
 * {@link Injector}. Output format is:
 *
 * <pre>
 * https://example.com/ \t nutch.score=1.0
 * </pre>
 *
 * Exported items from CrawlDb can be selected/filtered using the options
 * available in &quot;CrawlDbReader -dump&quot;.
 */
public class CrawlDbToSeeds extends CrawlDbReader {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static class CrawlDbToSeedsOutputFormat
      extends FileOutputFormat<Text, CrawlDatum> {

    protected static class LineRecordWriter
        extends RecordWriter<Text, CrawlDatum> {

      private DataOutputStream out;

      public LineRecordWriter(DataOutputStream out) {
        this.out = out;
      }

      protected float normalizeScore(float score) {
        if (score > 10.0) {
          return 10.0f;
        }
        return score;
      }

      public synchronized void write(Text key, CrawlDatum value)
          throws IOException {
        out.writeBytes(key.toString());
        out.writeBytes("\tnutch.score=");
        out.writeBytes(Float.toString(normalizeScore(value.getScore())));
        out.writeByte('\n');
      }

      public synchronized void close(TaskAttemptContext context)
          throws IOException {
        out.close();
      }
    }

    public RecordWriter<Text, CrawlDatum> getRecordWriter(
        TaskAttemptContext context) throws IOException {
      String name = getUniqueFile(context, "part", "");
      Path dir = FileOutputFormat.getOutputPath(context);
      FileSystem fs = dir.getFileSystem(context.getConfiguration());
      DataOutputStream fileOut = fs.create(new Path(dir, name), context);
      return new LineRecordWriter(fileOut);
    }
  }

  public void crawlDbToSeeds(String crawlDb, String output, String regex,
      String status, Integer retry, String expr, Float sample)
      throws IOException, ClassNotFoundException, InterruptedException {

    LOG.info("CrawlDbToSeeds: starting");
    LOG.info("CrawlDb db: " + crawlDb);

    Path outFolder = new Path(output);

    Job job = NutchJob.getInstance(getConf());
    job.setJobName("dump " + crawlDb);
    Configuration jobConf = job.getConfiguration();

    FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileOutputFormat.setOutputPath(job, outFolder);
    job.setOutputFormatClass(CrawlDbToSeedsOutputFormat.class);

    if (status != null)
      jobConf.set("status", status);
    if (regex != null)
      jobConf.set("regex", regex);
    if (retry != null)
      jobConf.setInt("retry", retry);
    if (expr != null) {
      jobConf.set("expr", expr);
      LOG.info("CrawlDb db: expr: " + expr);
    }
    if (sample != null) {
      jobConf.setFloat("sample", sample);
    }
    job.setMapperClass(CrawlDbDumpMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);
    job.setJarByClass(CrawlDbToSeeds.class);

    try {
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "CrawlDbToSeeds job did not succeed, job status:"
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }

    LOG.info("CrawlDbToSeeds: done");
  }

  public static int help() {
    System.err.println("Usage: CrawlDbToSeeds [options] <crawldb> <outdir>\n");
    System.err.println(
        "Select items from CrawlDb and export URLs and score as Nutch seed file\n");
    System.err
        .println("\nOptions to filter records (cf. CrawlDbReader -dump):");
    System.err.println("\t\t[-regex <expr>]\tfilter records with expression");
    System.err.println("\t\t[-retry <num>]\tminimum retry count");
    System.err
        .println("\t\t[-status <status>]\tfilter records by CrawlDatum status");
    System.err.println(
        "\t\t[-expr <expr>]\tJexl expression to evaluate for this record");
    System.err.println(
        "\t\t[-sample <fraction>]\tOnly process a random sample with this ratio");
    return -1;
  }

  public int run(String[] args) throws IOException, InterruptedException,
      ClassNotFoundException, Exception {

    if (args.length < 2) {
      return help();
    }

    String regex = null;
    Integer retry = null;
    String status = null;
    String expr = null;
    Float sample = null;
    String outputDir = null;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-regex")) {
        regex = args[++i];
      } else if (args[i].equals("-retry")) {
        retry = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-status")) {
        status = args[++i];
      } else if (args[i].equals("-expr")) {
        expr = args[++i];
      } else if (args[i].equals("-sample")) {
        sample = Float.parseFloat(args[++i]);
      } else if (crawlDb == null) {
        crawlDb = args[i];
      } else if (outputDir == null) {
        outputDir = args[i];
      } else {
        System.err.println("Unknown argument: " + args[i] + "\n\n");
        return help();
      }
    }
    crawlDbToSeeds(crawlDb, outputDir, regex, status, retry, expr, sample);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(NutchConfiguration.create(),
        new CrawlDbToSeeds(), args);
    System.exit(result);
  }

}

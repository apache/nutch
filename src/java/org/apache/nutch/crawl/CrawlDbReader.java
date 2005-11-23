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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.apache.nutch.fs.NutchFileSystem;
import org.apache.nutch.io.FloatWritable;
import org.apache.nutch.io.IntWritable;
import org.apache.nutch.io.LongWritable;
import org.apache.nutch.io.MapFile;
import org.apache.nutch.io.SequenceFile;
import org.apache.nutch.io.UTF8;
import org.apache.nutch.io.Writable;
import org.apache.nutch.io.WritableComparable;
import org.apache.nutch.io.MapFile.Reader;
import org.apache.nutch.mapred.JobClient;
import org.apache.nutch.mapred.JobConf;
import org.apache.nutch.mapred.MapFileOutputFormat;
import org.apache.nutch.mapred.Mapper;
import org.apache.nutch.mapred.OutputCollector;
import org.apache.nutch.mapred.Reducer;
import org.apache.nutch.mapred.Reporter;
import org.apache.nutch.mapred.SequenceFileInputFormat;
import org.apache.nutch.mapred.SequenceFileOutputFormat;
import org.apache.nutch.mapred.TextOutputFormat;
import org.apache.nutch.mapred.lib.HashPartitioner;
import org.apache.nutch.mapred.lib.LongSumReducer;
import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.util.NutchConf;

/**
 * Read utility for the CrawlDB.
 * 
 * @author Andrzej Bialecki
 * 
 */
public class CrawlDbReader {

  public static final Logger LOG = LogFormatter.getLogger(CrawlDbReader.class.getName());

  public static class CrawlDbStatMapper implements Mapper {
    public void configure(JobConf job) {}
    public void map(WritableComparable key, Writable value, OutputCollector output, Reporter reporter)
            throws IOException {
      CrawlDatum cd = (CrawlDatum) value;
      output.collect(new UTF8("TOTAL urls"), new LongWritable(1));
      output.collect(new UTF8("status"), new LongWritable(cd.getStatus()));
      output.collect(new UTF8("retry"), new LongWritable(cd.getRetriesSinceFetch()));
      output.collect(new UTF8("score"), new LongWritable((long) (cd.getScore() * 1000.0)));
    }
  }

  public static class CrawlDbStatReducer implements Reducer {
    public void configure(JobConf job) {}
    public void reduce(WritableComparable key, Iterator values, OutputCollector output, Reporter reporter)
            throws IOException {

      String k = ((UTF8) key).toString();
      if (k.equals("TOTAL urls")) {
        // sum all values for this key
        long sum = 0;
        while (values.hasNext()) {
          sum += ((LongWritable) values.next()).get();
        }
        // output sum
        output.collect(key, new LongWritable(sum));
      } else if (k.equals("status") || k.equals("retry")) {
        TreeMap stats = new TreeMap();
        while (values.hasNext()) {
          long val = ((LongWritable) values.next()).get();
          LongWritable cnt = (LongWritable) stats.get(k + " " + val);
          if (cnt == null) {
            cnt = new LongWritable();
            stats.put(k + " " + val, cnt);
          }
          cnt.set(cnt.get() + 1);
        }
        Iterator it = stats.keySet().iterator();
        while (it.hasNext()) {
          String s = (String) it.next();
          output.collect(new UTF8(s), (LongWritable) stats.get(s));
        }
      } else if (k.equals("score")) {
        long min = 0, max = 0, avg = 0, total = 0;
        int cnt = 0;
        boolean first = true;
        while (values.hasNext()) {
          long val = ((LongWritable) values.next()).get();
          if (first) {
            min = val;
            max = val;
            first = false;
          }
          if (val < min) min = val;
          if (val > max) max = val;
          total += val;
          cnt++;
        }
        output.collect(new UTF8("max score"), new LongWritable(max));
        output.collect(new UTF8("min score"), new LongWritable(min));
        output.collect(new UTF8("avg score"), new LongWritable(total / cnt));
      }
    }
  }

  public static class CrawlDbDumpReducer implements Reducer {

    public void reduce(WritableComparable key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
      while (values.hasNext()) {
        output.collect(key, (Writable)values.next());
      }
    }

    public void configure(JobConf job) {
    }
  }
  
  public void processStatJob(String crawlDb, NutchConf config) throws IOException {
    LOG.info("CrawlDb statistics start: " + crawlDb);
    File tmpFolder = new File(crawlDb, "stat_tmp" + System.currentTimeMillis());

    JobConf job = new JobConf(config);

    job.addInputDir(new File(crawlDb, CrawlDatum.DB_DIR_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(CrawlDatum.class);

    job.setMapperClass(CrawlDbStatMapper.class);
    job.setReducerClass(CrawlDbStatReducer.class);

    job.setOutputDir(tmpFolder);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(LongWritable.class);

    JobClient.runJob(job);

    // reading the result
    NutchFileSystem fileSystem = NutchFileSystem.get();
    SequenceFile.Reader[] readers = SequenceFileOutputFormat.getReaders(fileSystem, tmpFolder);

    UTF8 key = new UTF8();
    LongWritable value = new LongWritable();

    TreeMap stats = new TreeMap();
    int avg = 0, min = 0, max = 0;
    for (int i = 0; i < readers.length; i++) {
      SequenceFile.Reader reader = readers[i];
      while (reader.next(key, value)) {
        String k = key.toString();
        LongWritable val = (LongWritable) stats.get(k);
        if (val == null) {
          val = new LongWritable();
          stats.put(k, val);
        }
        val.set(val.get() + value.get());
        if (k.startsWith("max"))
          max++;
        else if (k.startsWith("min"))
          min++;
        else if (k.startsWith("avg")) avg++;
      }
    }
    LOG.info("Statistics for CrawlDb: " + crawlDb);
    Iterator it = stats.keySet().iterator();
    while (it.hasNext()) {
      String k = (String) it.next();
      LongWritable val = (LongWritable) stats.get(k);
      if (k.contains("score")) {
        if (k.startsWith("min")) {
          LOG.info(k + ":\t" + (float) ((float) (val.get() / min) / 1000.0f));
        } else if (k.startsWith("max")) {
          LOG.info(k + ":\t" + (float) ((float) (val.get() / max) / 1000.0f));
        } else if (k.startsWith("avg")) {
          LOG.info(k + ":\t" + (float) ((float) (val.get() / avg) / 1000.0f));
        }
      } else if (k.startsWith("status")) {
        int code = Integer.parseInt(k.substring(k.indexOf(' ') + 1));
        LOG.info(k + " (" + CrawlDatum.statNames[code] + "):\t" + val);
      } else LOG.info(k + ":\t" + val);
    }
    // removing the tmp folder
    fileSystem.delete(tmpFolder);
    LOG.info("CrawlDb statistics: done");

  }

  public void readUrl(String crawlDb, String url, NutchConf config) throws IOException {
    NutchFileSystem fs = NutchFileSystem.get(config);
    UTF8 key = new UTF8(url);
    CrawlDatum val = new CrawlDatum();
    MapFile.Reader[] readers = MapFileOutputFormat.getReaders(fs, new File(crawlDb, CrawlDatum.DB_DIR_NAME));
    Writable res = MapFileOutputFormat.getEntry(readers, new HashPartitioner(), key, val);
    System.out.println("URL: " + url);
    if (res != null) {
      System.out.println(val);
    } else {
      System.out.println("not found");
    }
  }
  
  public void processDumpJob(String crawlDb, String output, NutchConf config) throws IOException {

    LOG.info("CrawlDb dump: starting");
    LOG.info("CrawlDb db: " + crawlDb);
    File outFolder = new File(output);

    JobConf job = new JobConf(config);

    job.addInputDir(new File(crawlDb, CrawlDatum.DB_DIR_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(CrawlDatum.class);

    job.setOutputDir(outFolder);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(CrawlDatum.class);

    JobClient.runJob(job);
  }

  public static void main(String[] args) throws IOException {
    CrawlDbReader dbr = new CrawlDbReader();

    if (args.length < 1) {
      System.err.println("Usage: CrawlDbReader <crawldb> (-stats | -dump <out_dir> | -url <url>)");
      System.err.println("\t<crawldb>\tdirectory name where crawldb is located");
      System.err.println("\t-stats\tprint overall statistics to System.out");
      System.err.println("\t-dump <out_dir>\tdump the whole db to a text file in <out_dir>");
      System.err.println("\t-url <url>\tprint information on <url> to System.out");
      return;
    }
    String param = null;
    String crawlDb = args[0];
    NutchConf conf = NutchConf.get();
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-stats")) {
        dbr.processStatJob(crawlDb, conf);
      } else if (args[i].equals("-dump")) {
        param = args[++i];
        dbr.processDumpJob(crawlDb, param, conf);
      } else if (args[i].equals("-url")) {
        param = args[++i];
        dbr.readUrl(crawlDb, param, conf);
      } else {
        System.err.println("\nError: wrong argument " + args[i]);
      }
    }
    return;
  }
}

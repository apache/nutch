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

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Closeable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/**
 * Read utility for the CrawlDB.
 * 
 * @author Andrzej Bialecki
 * 
 */
public class CrawlDbReader implements Closeable {

  public static final Log LOG = LogFactory.getLog(CrawlDbReader.class);
  
  private MapFile.Reader[] readers = null;
  
  private void openReaders(String crawlDb, Configuration config) throws IOException {
    if (readers != null) return;
    FileSystem fs = FileSystem.get(config);
    readers = MapFileOutputFormat.getReaders(fs, new Path(crawlDb, CrawlDatum.DB_DIR_NAME), config);
  }
  
  private void closeReaders() {
    if (readers == null) return;
    for (int i = 0; i < readers.length; i++) {
      try {
        readers[i].close();
      } catch (Exception e) {
        
      }
    }
  }

  public static class CrawlDbStatMapper implements Mapper {
    public void configure(JobConf job) {}
    public void close() {}
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
    public void close() {}
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
        long min = 0, max = 0, total = 0;
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

    public void configure(JobConf job) {}
    public void close() {}
  }
  
  public static class CrawlDbTopNMapper implements Mapper {
    private static final FloatWritable fw = new FloatWritable();
    private float min = 0.0f;
    
    public void configure(JobConf job) {
      long lmin = job.getLong("CrawlDbReader.topN.min", 0);
      if (lmin != 0) {
        min = (float)lmin / 1000000.0f;
      }
    }
    public void close() {}
    public void map(WritableComparable key, Writable value, OutputCollector output, Reporter reporter)
            throws IOException {
      CrawlDatum datum = (CrawlDatum)value;
      if (datum.getScore() < min) return; // don't collect low-scoring records
      fw.set(-datum.getScore()); // reverse sorting order
      output.collect(fw, key); // invert mapping: score -> url
    }
  }
  
  public static class CrawlDbTopNReducer implements Reducer {
    private long topN;
    private long count = 0L;
    
    public void reduce(WritableComparable key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
      while (values.hasNext() && count < topN) {
        FloatWritable fw = (FloatWritable)key;
        fw.set(-fw.get());
        output.collect(fw, (Writable)values.next());
        count++;
      }
    }

    public void configure(JobConf job) {
      topN = job.getLong("CrawlDbReader.topN", 100) / job.getNumReduceTasks();
    }
    
    public void close() {}
  }

  public void close() {
    closeReaders();
  }
  
  public void processStatJob(String crawlDb, Configuration config) throws IOException {
    LOG.info("CrawlDb statistics start: " + crawlDb);
    Path tmpFolder = new Path(crawlDb, "stat_tmp" + System.currentTimeMillis());

    JobConf job = new NutchJob(config);
    job.setJobName("stats " + crawlDb);

    job.addInputPath(new Path(crawlDb, CrawlDatum.DB_DIR_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(CrawlDatum.class);

    job.setMapperClass(CrawlDbStatMapper.class);
    job.setReducerClass(CrawlDbStatReducer.class);

    job.setOutputPath(tmpFolder);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(LongWritable.class);

    JobClient.runJob(job);

    // reading the result
    FileSystem fileSystem = FileSystem.get(config);
    SequenceFile.Reader[] readers = SequenceFileOutputFormat.getReaders(config, tmpFolder);

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
      if (k.indexOf("score") != -1) {
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
  
  public CrawlDatum get(String crawlDb, String url, Configuration config) throws IOException {
    UTF8 key = new UTF8(url);
    CrawlDatum val = new CrawlDatum();
    openReaders(crawlDb, config);
    CrawlDatum res = (CrawlDatum)MapFileOutputFormat.getEntry(readers, new HashPartitioner(), key, val);
    return res;
  }

  public void readUrl(String crawlDb, String url, Configuration config) throws IOException {
    CrawlDatum res = get(crawlDb, url, config);
    System.out.println("URL: " + url);
    if (res != null) {
      System.out.println(res);
    } else {
      System.out.println("not found");
    }
  }
  
  public void processDumpJob(String crawlDb, String output, Configuration config) throws IOException {

    LOG.info("CrawlDb dump: starting");
    LOG.info("CrawlDb db: " + crawlDb);
    Path outFolder = new Path(output);

    JobConf job = new NutchJob(config);
    job.setJobName("dump " + crawlDb);

    job.addInputPath(new Path(crawlDb, CrawlDatum.DB_DIR_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(CrawlDatum.class);

    job.setOutputPath(outFolder);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(CrawlDatum.class);

    JobClient.runJob(job);
    LOG.info("CrawlDb dump: done");
  }

  public void processTopNJob(String crawlDb, long topN, float min, String output, Configuration config) throws IOException {
    LOG.info("CrawlDb topN: starting (topN=" + topN + ", min=" + min + ")");
    LOG.info("CrawlDb db: " + crawlDb);
    Path outFolder = new Path(output);
    Path tempDir =
      new Path(config.get("mapred.temp.dir", ".") +
               "/readdb-topN-temp-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.addInputPath(new Path(crawlDb, CrawlDatum.DB_DIR_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(CrawlDatum.class);
    job.setMapperClass(CrawlDbTopNMapper.class);
    job.setReducerClass(IdentityReducer.class);

    job.setOutputPath(tempDir);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(UTF8.class);

    // XXX hmmm, no setFloat() in the API ... :(
    job.setLong("CrawlDbReader.topN.min", Math.round(1000000.0 * min));
    JobClient.runJob(job); 
    
    LOG.info("CrawlDb topN: collecting topN scores.");
    job = new NutchJob(config);
    job.setLong("CrawlDbReader.topN", topN);

    job.addInputPath(tempDir);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(FloatWritable.class);
    job.setInputValueClass(UTF8.class);
    job.setMapperClass(IdentityMapper.class);
    job.setReducerClass(CrawlDbTopNReducer.class);

    job.setOutputPath(outFolder);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(UTF8.class);

    // XXX *sigh* this apparently doesn't work ... :-((
    job.setNumReduceTasks(1); // create a single file.
    
    JobClient.runJob(job);
    FileSystem fs = FileSystem.get(config);
    fs.delete(tempDir);
    LOG.info("CrawlDb topN: done");

  }

  public static void main(String[] args) throws IOException {
    CrawlDbReader dbr = new CrawlDbReader();

    if (args.length < 1) {
      System.err.println("Usage: CrawlDbReader <crawldb> (-stats | -dump <out_dir> | -topN <nnnn> <out_dir> [<min>] | -url <url>)");
      System.err.println("\t<crawldb>\tdirectory name where crawldb is located");
      System.err.println("\t-stats\tprint overall statistics to System.out");
      System.err.println("\t-dump <out_dir>\tdump the whole db to a text file in <out_dir>");
      System.err.println("\t-url <url>\tprint information on <url> to System.out");
      System.err.println("\t-topN <nnnn> <out_dir> [<min>]\tdump top <nnnn> urls sorted by score to <out_dir>");
      System.err.println("\t\t[<min>]\tskip records with scores below this value.");
      System.err.println("\t\t\tThis can significantly improve performance.");
      return;
    }
    String param = null;
    String crawlDb = args[0];
    Configuration conf = NutchConfiguration.create();
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-stats")) {
        dbr.processStatJob(crawlDb, conf);
      } else if (args[i].equals("-dump")) {
        param = args[++i];
        dbr.processDumpJob(crawlDb, param, conf);
      } else if (args[i].equals("-url")) {
        param = args[++i];
        dbr.readUrl(crawlDb, param, conf);
      } else if (args[i].equals("-topN")) {
        param = args[++i];
        long topN = Long.parseLong(param);
        param = args[++i];
        float min = 0.0f;
        if (i < args.length - 1) {
          min = Float.parseFloat(args[++i]);
        }
        dbr.processTopNJob(crawlDb, topN, min, param, conf);
      } else {
        System.err.println("\nError: wrong argument " + args[i]);
      }
    }
    return;
  }
}

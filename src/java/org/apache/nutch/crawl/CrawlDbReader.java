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

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormatBase;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Progressable;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.StringUtil;

/**
 * Read utility for the CrawlDB.
 * 
 * @author Andrzej Bialecki
 * 
 */
public class CrawlDbReader implements Closeable {

  public static final Log LOG = LogFactory.getLog(CrawlDbReader.class);
  
  public static final int STD_FORMAT = 0;
  public static final int CSV_FORMAT = 1;
    
  private MapFile.Reader[] readers = null;
  
  private void openReaders(String crawlDb, Configuration config) throws IOException {
    if (readers != null) return;
    FileSystem fs = FileSystem.get(config);
    readers = MapFileOutputFormat.getReaders(fs, new Path(crawlDb, CrawlDb.CURRENT_NAME), config);
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
  
  public static class CrawlDatumCsvOutputFormat extends OutputFormatBase<Text,CrawlDatum> {
    protected static class LineRecordWriter implements RecordWriter<Text,CrawlDatum> {
      private DataOutputStream out;

      public LineRecordWriter(DataOutputStream out) {
        this.out = out;
        try {
          out.writeBytes("Url;Status code;Status name;Fetch Time;Modified Time;Retries since fetch;Retry interval;Score;Signature;Metadata\n");
        } catch (IOException e) {}
      }

      public synchronized void write(Text key, CrawlDatum value) throws IOException {
          out.writeByte('"');
          out.writeBytes(key.toString());
          out.writeByte('"');
          out.writeByte(';');
          out.writeBytes(Integer.toString(value.getStatus()));
          out.writeByte(';');
          out.writeByte('"');
          out.writeBytes(CrawlDatum.getStatusName(value.getStatus()));
          out.writeByte('"');
          out.writeByte(';');
          out.writeBytes(new Date(value.getFetchTime()).toString());
          out.writeByte(';');
          out.writeBytes(new Date(value.getModifiedTime()).toString());
          out.writeByte(';');
          out.writeBytes(Integer.toString(value.getRetriesSinceFetch()));
          out.writeByte(';');
          out.writeBytes(Float.toString(value.getFetchInterval()));
          out.writeByte(';');
          out.writeBytes(Float.toString((value.getFetchInterval() / FetchSchedule.SECONDS_PER_DAY)));
          out.writeByte(';');
          out.writeBytes(Float.toString(value.getScore()));
          out.writeByte(';');
          out.writeByte('"');
          out.writeBytes(value.getSignature() != null ? StringUtil.toHexString(value.getSignature()): "null");
          out.writeByte('"');
          out.writeByte('\n');
      }

      public synchronized void close(Reporter reporter) throws IOException {
        out.close();
      }
    }

    public RecordWriter<Text,CrawlDatum> getRecordWriter(FileSystem fs, JobConf job, String name,
        Progressable progress) throws IOException {
      Path dir = job.getOutputPath();
      DataOutputStream fileOut = fs.create(new Path(dir, name), progress);
      return new LineRecordWriter(fileOut);
   }
  }

  public static class CrawlDbStatMapper implements Mapper<Text, CrawlDatum, Text, LongWritable> {
    LongWritable COUNT_1 = new LongWritable(1);
    private boolean sort = false;
    public void configure(JobConf job) {
      sort = job.getBoolean("db.reader.stats.sort", false );
    }
    public void close() {}
    public void map(Text key, CrawlDatum value, OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {
      output.collect(new Text("T"), COUNT_1);
      output.collect(new Text("status " + value.getStatus()), COUNT_1);
      output.collect(new Text("retry " + value.getRetriesSinceFetch()), COUNT_1);
      output.collect(new Text("s"), new LongWritable((long) (value.getScore() * 1000.0)));
      if(sort){
        URL u = new URL(key.toString());
        String host = u.getHost();
        output.collect(new Text("status " + value.getStatus() + " " + host), COUNT_1);
      }
    }
  }
  
  public static class CrawlDbStatCombiner implements Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable val = new LongWritable();
    
    public CrawlDbStatCombiner() { }
    public void configure(JobConf job) { }
    public void close() {}
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter)
        throws IOException {
      val.set(0L);
      String k = ((Text)key).toString();
      if (!k.equals("s")) {
        while (values.hasNext()) {
          LongWritable cnt = (LongWritable)values.next();
          val.set(val.get() + cnt.get());
        }
        output.collect(key, val);
      } else {
        long total = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        while (values.hasNext()) {
          LongWritable cnt = (LongWritable)values.next();
          if (cnt.get() < min) min = cnt.get();
          if (cnt.get() > max) max = cnt.get();
          total += cnt.get();
        }
        output.collect(new Text("scn"), new LongWritable(min));
        output.collect(new Text("scx"), new LongWritable(max));
        output.collect(new Text("sct"), new LongWritable(total));
      }
    }
  }

  public static class CrawlDbStatReducer implements Reducer<Text, LongWritable, Text, LongWritable> {
    public void configure(JobConf job) {}
    public void close() {}
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {

      String k = ((Text) key).toString();
      if (k.equals("T")) {
        // sum all values for this key
        long sum = 0;
        while (values.hasNext()) {
          sum += ((LongWritable) values.next()).get();
        }
        // output sum
        output.collect(key, new LongWritable(sum));
      } else if (k.startsWith("status") || k.startsWith("retry")) {
        LongWritable cnt = new LongWritable();
        while (values.hasNext()) {
          LongWritable val = (LongWritable)values.next();
          cnt.set(cnt.get() + val.get());
        }
        output.collect(key, cnt);
      } else if (k.equals("scx")) {
        LongWritable cnt = new LongWritable(Long.MIN_VALUE);
        while (values.hasNext()) {
          LongWritable val = (LongWritable)values.next();
          if (cnt.get() < val.get()) cnt.set(val.get());
        }
        output.collect(key, cnt);
      } else if (k.equals("scn")) {
        LongWritable cnt = new LongWritable(Long.MAX_VALUE);
        while (values.hasNext()) {
          LongWritable val = (LongWritable)values.next();
          if (cnt.get() > val.get()) cnt.set(val.get());
        }
        output.collect(key, cnt);
      } else if (k.equals("sct")) {
        LongWritable cnt = new LongWritable();
        while (values.hasNext()) {
          LongWritable val = (LongWritable)values.next();
          cnt.set(cnt.get() + val.get());
        }
        output.collect(key, cnt);
      }
    }
  }

  public static class CrawlDbTopNMapper implements Mapper<Text, CrawlDatum, FloatWritable, Text> {
    private static final FloatWritable fw = new FloatWritable();
    private float min = 0.0f;
    
    public void configure(JobConf job) {
      long lmin = job.getLong("db.reader.topn.min", 0);
      if (lmin != 0) {
        min = (float)lmin / 1000000.0f;
      }
    }
    public void close() {}
    public void map(Text key, CrawlDatum value, OutputCollector<FloatWritable, Text> output, Reporter reporter)
            throws IOException {
      if (value.getScore() < min) return; // don't collect low-scoring records
      fw.set(-value.getScore()); // reverse sorting order
      output.collect(fw, key); // invert mapping: score -> url
    }
  }
  
  public static class CrawlDbTopNReducer implements Reducer<FloatWritable, Text, FloatWritable, Text> {
    private long topN;
    private long count = 0L;
    
    public void reduce(FloatWritable key, Iterator<Text> values, OutputCollector<FloatWritable, Text> output, Reporter reporter) throws IOException {
      while (values.hasNext() && count < topN) {
        key.set(-key.get());
        output.collect(key, values.next());
        count++;
      }
    }

    public void configure(JobConf job) {
      topN = job.getLong("db.reader.topn", 100) / job.getNumReduceTasks();
    }
    
    public void close() {}
  }

  public void close() {
    closeReaders();
  }
  
  public void processStatJob(String crawlDb, Configuration config, boolean sort) throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb statistics start: " + crawlDb);
    }
    
    Path tmpFolder = new Path(crawlDb, "stat_tmp" + System.currentTimeMillis());

    JobConf job = new NutchJob(config);
    job.setJobName("stats " + crawlDb);
    job.setBoolean("db.reader.stats.sort", sort);

    job.addInputPath(new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(CrawlDbStatMapper.class);
    job.setCombinerClass(CrawlDbStatCombiner.class);
    job.setReducerClass(CrawlDbStatReducer.class);

    job.setOutputPath(tmpFolder);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    JobClient.runJob(job);

    // reading the result
    FileSystem fileSystem = FileSystem.get(config);
    SequenceFile.Reader[] readers = SequenceFileOutputFormat.getReaders(config, tmpFolder);

    Text key = new Text();
    LongWritable value = new LongWritable();

    TreeMap<String, LongWritable> stats = new TreeMap<String, LongWritable>();
    for (int i = 0; i < readers.length; i++) {
      SequenceFile.Reader reader = readers[i];
      while (reader.next(key, value)) {
        String k = key.toString();
        LongWritable val = stats.get(k);
        if (val == null) {
          val = new LongWritable();
          if (k.equals("scx")) val.set(Long.MIN_VALUE);
          if (k.equals("scn")) val.set(Long.MAX_VALUE);
          stats.put(k, val);
        }
        if (k.equals("scx")) {
          if (val.get() < value.get()) val.set(value.get());
        } else if (k.equals("scn")) {
          if (val.get() > value.get()) val.set(value.get());          
        } else {
          val.set(val.get() + value.get());
        }
      }
      reader.close();
    }
    
    if (LOG.isInfoEnabled()) {
      LOG.info("Statistics for CrawlDb: " + crawlDb);
      LongWritable totalCnt = stats.get("T");
      stats.remove("T");
      LOG.info("TOTAL urls:\t" + totalCnt.get());
      for (Map.Entry<String, LongWritable> entry : stats.entrySet()) {
        String k = entry.getKey();
        LongWritable val = entry.getValue();
        if (k.equals("scn")) {
          LOG.info("min score:\t" + (float) (val.get() / 1000.0f));
        } else if (k.equals("scx")) {
          LOG.info("max score:\t" + (float) (val.get() / 1000.0f));
        } else if (k.equals("sct")) {
          LOG.info("avg score:\t" + (float) ((((double)val.get()) / totalCnt.get()) / 1000.0));
        } else if (k.startsWith("status")) {
          String[] st = k.split(" ");
          int code = Integer.parseInt(st[1]);
          if(st.length >2 ) LOG.info("   " + st[2] +" :\t" + val);
          else LOG.info(st[0] +" " +code + " (" + CrawlDatum.getStatusName((byte) code) + "):\t" + val);
        } else LOG.info(k + ":\t" + val);
      }
    }
    // removing the tmp folder
    fileSystem.delete(tmpFolder);
    if (LOG.isInfoEnabled()) { LOG.info("CrawlDb statistics: done"); }

  }
  
  public CrawlDatum get(String crawlDb, String url, Configuration config) throws IOException {
    Text key = new Text(url);
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
  
  public void processDumpJob(String crawlDb, String output, Configuration config, int format) throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb dump: starting");
      LOG.info("CrawlDb db: " + crawlDb);
    }
    
    Path outFolder = new Path(output);

    JobConf job = new NutchJob(config);
    job.setJobName("dump " + crawlDb);

    job.addInputPath(new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setOutputPath(outFolder);
    if(format == CSV_FORMAT) job.setOutputFormat(CrawlDatumCsvOutputFormat.class);
    else job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    JobClient.runJob(job);
    if (LOG.isInfoEnabled()) { LOG.info("CrawlDb dump: done"); }
  }

  public void processTopNJob(String crawlDb, long topN, float min, String output, Configuration config) throws IOException {
    
    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb topN: starting (topN=" + topN + ", min=" + min + ")");
      LOG.info("CrawlDb db: " + crawlDb);
    }
    
    Path outFolder = new Path(output);
    Path tempDir =
      new Path(config.get("mapred.temp.dir", ".") +
               "/readdb-topN-temp-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.setJobName("topN prepare " + crawlDb);
    job.addInputPath(new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(CrawlDbTopNMapper.class);
    job.setReducerClass(IdentityReducer.class);

    job.setOutputPath(tempDir);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);

    // XXX hmmm, no setFloat() in the API ... :(
    job.setLong("db.reader.topn.min", Math.round(1000000.0 * min));
    JobClient.runJob(job); 
    
    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb topN: collecting topN scores.");
    }
    job = new NutchJob(config);
    job.setJobName("topN collect " + crawlDb);
    job.setLong("db.reader.topn", topN);

    job.addInputPath(tempDir);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(IdentityMapper.class);
    job.setReducerClass(CrawlDbTopNReducer.class);

    job.setOutputPath(outFolder);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);

    // XXX *sigh* this apparently doesn't work ... :-((
    job.setNumReduceTasks(1); // create a single file.
    
    JobClient.runJob(job);
    FileSystem fs = FileSystem.get(config);
    fs.delete(tempDir);
    if (LOG.isInfoEnabled()) { LOG.info("CrawlDb topN: done"); }

  }

  public static void main(String[] args) throws IOException {
    CrawlDbReader dbr = new CrawlDbReader();

    if (args.length < 1) {
      System.err.println("Usage: CrawlDbReader <crawldb> (-stats | -dump <out_dir> | -topN <nnnn> <out_dir> [<min>] | -url <url>)");
      System.err.println("\t<crawldb>\tdirectory name where crawldb is located");
      System.err.println("\t-stats [-sort] \tprint overall statistics to System.out");
      System.err.println("\t\t[-sort]\tlist status sorted by host");
      System.err.println("\t-dump <out_dir> [-format normal|csv ]\tdump the whole db to a text file in <out_dir>");
      System.err.println("\t\t[-format csv]\tdump in Csv format");
      System.err.println("\t\t[-format normal]\tdump in standard format (default option)");
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
        boolean toSort = false;
        if(i < args.length - 1 && "-sort".equals(args[i+1])){
          toSort = true;
          i++;
        }
        dbr.processStatJob(crawlDb, conf, toSort);
      } else if (args[i].equals("-dump")) {
        param = args[++i];
        String format = "normal";
        if(i < args.length - 1 &&  "-format".equals(args[i+1]))
          format = args[i=i+2];
        dbr.processDumpJob(crawlDb, param, conf, "csv".equals(format)? CSV_FORMAT : STD_FORMAT );
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

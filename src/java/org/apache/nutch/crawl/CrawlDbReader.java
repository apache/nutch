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
import java.io.File;
import java.io.IOException;
import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.TreeMap;


// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.JexlUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TimingUtil;
import org.apache.commons.jexl2.Expression;

/**
 * Read utility for the CrawlDB.
 * 
 * @author Andrzej Bialecki
 * 
 */
public class CrawlDbReader extends Configured implements Closeable, Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private MapFile.Reader[] readers = null;

  private void openReaders(String crawlDb, JobConf config)
      throws IOException {
    if (readers != null)
      return;
    Path crawlDbPath = new Path(crawlDb, CrawlDb.CURRENT_NAME);
    FileSystem fs = crawlDbPath.getFileSystem(config);
    readers = MapFileOutputFormat.getReaders(fs, crawlDbPath, config);
  }

  private void closeReaders() {
    if (readers == null)
      return;
    for (int i = 0; i < readers.length; i++) {
      try {
        readers[i].close();
      } catch (Exception e) {

      }
    }
  }

  public static class CrawlDatumCsvOutputFormat extends
      FileOutputFormat<Text, CrawlDatum> {
    protected static class LineRecordWriter implements
        RecordWriter<Text, CrawlDatum> {
      private DataOutputStream out;

      public LineRecordWriter(DataOutputStream out) {
        this.out = out;
        try {
          out.writeBytes("Url,Status code,Status name,Fetch Time,Modified Time,Retries since fetch,Retry interval seconds,Retry interval days,Score,Signature,Metadata\n");
        } catch (IOException e) {
        }
      }

      public synchronized void write(Text key, CrawlDatum value)
          throws IOException {
        out.writeByte('"');
        out.writeBytes(key.toString());
        out.writeByte('"');
        out.writeByte(',');
        out.writeBytes(Integer.toString(value.getStatus()));
        out.writeByte(',');
        out.writeByte('"');
        out.writeBytes(CrawlDatum.getStatusName(value.getStatus()));
        out.writeByte('"');
        out.writeByte(',');
        out.writeBytes(new Date(value.getFetchTime()).toString());
        out.writeByte(',');
        out.writeBytes(new Date(value.getModifiedTime()).toString());
        out.writeByte(',');
        out.writeBytes(Integer.toString(value.getRetriesSinceFetch()));
        out.writeByte(',');
        out.writeBytes(Float.toString(value.getFetchInterval()));
        out.writeByte(',');
        out.writeBytes(Float.toString((value.getFetchInterval() / FetchSchedule.SECONDS_PER_DAY)));
        out.writeByte(',');
        out.writeBytes(Float.toString(value.getScore()));
        out.writeByte(',');
        out.writeByte('"');
        out.writeBytes(value.getSignature() != null ? StringUtil
            .toHexString(value.getSignature()) : "null");
        out.writeByte('"');
        out.writeByte(',');
        out.writeByte('"');
        if (value.getMetaData() != null) {
          for (Entry<Writable, Writable> e : value.getMetaData().entrySet()) {
            out.writeBytes(e.getKey().toString());
            out.writeByte(':');
            out.writeBytes(e.getValue().toString());
            out.writeBytes("|||");
          }
        }
        out.writeByte('"');

        out.writeByte('\n');
      }

      public synchronized void close(Reporter reporter) throws IOException {
        out.close();
      }
    }

    public RecordWriter<Text, CrawlDatum> getRecordWriter(FileSystem fs,
        JobConf job, String name, Progressable progress) throws IOException {
      Path dir = FileOutputFormat.getOutputPath(job);
      DataOutputStream fileOut = fs.create(new Path(dir, name), progress);
      return new LineRecordWriter(fileOut);
    }
  }

  public static class CrawlDbStatMapper
      implements Mapper<Text, CrawlDatum, Text, NutchWritable> {
    NutchWritable COUNT_1 = new NutchWritable(new LongWritable(1));
    private boolean sort = false;

    public void configure(JobConf job) {
      sort = job.getBoolean("db.reader.stats.sort", false);
    }

    public void close() {
    }

    public void map(Text key, CrawlDatum value,
        OutputCollector<Text, NutchWritable> output, Reporter reporter)
        throws IOException {
      output.collect(new Text("T"), COUNT_1);
      output.collect(new Text("status " + value.getStatus()), COUNT_1);
      output.collect(new Text("retry " + value.getRetriesSinceFetch()),
          COUNT_1);
      output.collect(new Text("sc"), new NutchWritable(
          new FloatWritable(value.getScore())));
      // fetch time (in minutes to prevent from overflows when summing up)
      output.collect(new Text("ft"), new NutchWritable(
          new LongWritable(value.getFetchTime() / (1000 * 60))));
      // fetch interval (in seconds)
      output.collect(new Text("fi"),
          new NutchWritable(new LongWritable(value.getFetchInterval())));
      if (sort) {
        URL u = new URL(key.toString());
        String host = u.getHost();
        output.collect(new Text("status " + value.getStatus() + " " + host),
            COUNT_1);
      }
    }
  }

  public static class CrawlDbStatCombiner implements
      Reducer<Text, NutchWritable, Text, NutchWritable> {
    LongWritable val = new LongWritable();

    public CrawlDbStatCombiner() {
    }

    public void configure(JobConf job) {
    }

    public void close() {
    }

    private void reduceMinMaxTotal(String keyPrefix, Iterator<NutchWritable> values,
        OutputCollector<Text, NutchWritable> output, Reporter reporter)
        throws IOException {
      long total = 0;
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      while (values.hasNext()) {
        long cnt = ((LongWritable) values.next().get()).get();
        if (cnt < min)
          min = cnt;
        if (cnt > max)
          max = cnt;
        total += cnt;
      }
      output.collect(new Text(keyPrefix + "n"),
          new NutchWritable(new LongWritable(min)));
      output.collect(new Text(keyPrefix + "x"),
          new NutchWritable(new LongWritable(max)));
      output.collect(new Text(keyPrefix + "t"),
          new NutchWritable(new LongWritable(total)));
    }
    
    private void reduceMinMaxTotalFloat(String keyPrefix, Iterator<NutchWritable> values,
        OutputCollector<Text, NutchWritable> output, Reporter reporter)
        throws IOException {
      double total = 0;
      float min = Float.MAX_VALUE;
      float max = Float.MIN_VALUE;
      TDigest tdigest = TDigest.createMergingDigest(100.0);
      while (values.hasNext()) {
        float val = ((FloatWritable) values.next().get()).get();
        tdigest.add(val);
        if (val < min)
          min = val;
        if (val > max)
          max = val;
        total += val;
      }
      output.collect(new Text(keyPrefix + "n"),
          new NutchWritable(new FloatWritable(min)));
      output.collect(new Text(keyPrefix + "x"),
          new NutchWritable(new FloatWritable(max)));
      output.collect(new Text(keyPrefix + "t"),
          new NutchWritable(new FloatWritable((float) total)));
      ByteBuffer tdigestBytes = ByteBuffer.allocate(tdigest.smallByteSize());
      tdigest.asSmallBytes(tdigestBytes);
      output.collect(new Text(keyPrefix + "d"),
          new NutchWritable(new BytesWritable(tdigestBytes.array())));
    }

    public void reduce(Text key, Iterator<NutchWritable> values,
        OutputCollector<Text, NutchWritable> output, Reporter reporter)
        throws IOException {
      val.set(0L);
      String k = key.toString();
      if (k.equals("sc")) {
        reduceMinMaxTotalFloat(k, values, output, reporter);
      } else if (k.equals("ft") || k.equals("fi")) {
        reduceMinMaxTotal(k, values, output, reporter);
      } else {
        while (values.hasNext()) {
          LongWritable cnt = (LongWritable) values.next().get();
          val.set(val.get() + cnt.get());
        }
        output.collect(key, new NutchWritable(val));
      }
    }
  }

  public static class CrawlDbStatReducer implements
      Reducer<Text, NutchWritable, Text, NutchWritable> {
    public void configure(JobConf job) {
    }

    public void close() {
    }

    public void reduce(Text key, Iterator<NutchWritable> values,
        OutputCollector<Text, NutchWritable> output, Reporter reporter)
        throws IOException {

      String k = key.toString();
      if (k.equals("T")) {
        // sum all values for this key
        long sum = 0;
        while (values.hasNext()) {
          Writable value = values.next().get();
          sum += ((LongWritable) value).get();
        }
        // output sum
        output.collect(key, new NutchWritable(new LongWritable(sum)));
      } else if (k.startsWith("status") || k.startsWith("retry")) {
        LongWritable cnt = new LongWritable();
        while (values.hasNext()) {
          LongWritable val = (LongWritable) values.next().get();
          cnt.set(cnt.get() + val.get());
        }
        output.collect(key, new NutchWritable(cnt));
      } else if (k.equals("scx")) {
        FloatWritable max = new FloatWritable(Float.MIN_VALUE);
        while (values.hasNext()) {
          FloatWritable val = (FloatWritable) values.next().get();
          if (max.get() < val.get())
            max.set(val.get());
        }
        output.collect(key, new NutchWritable(max));
      } else if (k.equals("ftx") || k.equals("fix")) {
        LongWritable cnt = new LongWritable(Long.MIN_VALUE);
        while (values.hasNext()) {
          LongWritable val = (LongWritable) values.next().get();
          if (cnt.get() < val.get())
            cnt.set(val.get());
        }
        output.collect(key, new NutchWritable(cnt));
      } else if (k.equals("scn")) {
        FloatWritable min = new FloatWritable(Float.MAX_VALUE);
        while (values.hasNext()) {
          FloatWritable val = (FloatWritable) values.next().get();
          if (min.get() > val.get())
            min.set(val.get());
        }
        output.collect(key, new NutchWritable(min));
      } else if (k.equals("ftn") || k.equals("fin")) {
        LongWritable cnt = new LongWritable(Long.MAX_VALUE);
        while (values.hasNext()) {
          LongWritable val = (LongWritable) values.next().get();
          if (cnt.get() > val.get())
            cnt.set(val.get());
        }
        output.collect(key, new NutchWritable(cnt));
      } else if (k.equals("sct")) {
        FloatWritable cnt = new FloatWritable();
        while (values.hasNext()) {
          FloatWritable val = (FloatWritable) values.next().get();
          cnt.set(cnt.get() + val.get());
        }
        output.collect(key, new NutchWritable(cnt));
      } else if (k.equals("ftt") || k.equals("fit")) {
        LongWritable cnt = new LongWritable();
        while (values.hasNext()) {
          LongWritable val = (LongWritable) values.next().get();
          cnt.set(cnt.get() + val.get());
        }
        output.collect(key, new NutchWritable(cnt));
      } else if (k.equals("scd") || k.equals("ftd") || k.equals("fid")) {
        MergingDigest tdigest = null;
        while (values.hasNext()) {
          byte[] bytes = ((BytesWritable) values.next().get()).getBytes();
          MergingDigest tdig = MergingDigest.fromBytes(ByteBuffer.wrap(bytes));
          if (tdigest == null) {
            tdigest = tdig;
          } else {
            tdigest.add(tdig);
          }
        }
        ByteBuffer tdigestBytes = ByteBuffer.allocate(tdigest.smallByteSize());
        tdigest.asSmallBytes(tdigestBytes);
        output.collect(key,
            new NutchWritable(new BytesWritable(tdigestBytes.array())));
      }
    }
  }

  public static class CrawlDbTopNMapper implements
      Mapper<Text, CrawlDatum, FloatWritable, Text> {
    private static final FloatWritable fw = new FloatWritable();
    private float min = 0.0f;

    public void configure(JobConf job) {
      min = job.getFloat("db.reader.topn.min", 0.0f);
    }

    public void close() {
    }

    public void map(Text key, CrawlDatum value,
        OutputCollector<FloatWritable, Text> output, Reporter reporter)
        throws IOException {
      if (value.getScore() < min)
        return; // don't collect low-scoring records
      fw.set(-value.getScore()); // reverse sorting order
      output.collect(fw, key); // invert mapping: score -> url
    }
  }

  public static class CrawlDbTopNReducer implements
      Reducer<FloatWritable, Text, FloatWritable, Text> {
    private long topN;
    private long count = 0L;

    public void reduce(FloatWritable key, Iterator<Text> values,
        OutputCollector<FloatWritable, Text> output, Reporter reporter)
        throws IOException {
      while (values.hasNext() && count < topN) {
        key.set(-key.get());
        output.collect(key, values.next());
        count++;
      }
    }

    public void configure(JobConf job) {
      topN = job.getLong("db.reader.topn", 100) / job.getNumReduceTasks();
    }

    public void close() {
    }
  }

  public void close() {
    closeReaders();
  }

  private TreeMap<String, Writable> processStatJobHelper(String crawlDb, Configuration config, boolean sort) throws IOException{
	  Path tmpFolder = new Path(crawlDb, "stat_tmp" + System.currentTimeMillis());

	  JobConf job = new NutchJob(config);
	  job.setJobName("stats " + crawlDb);
	  job.setBoolean("db.reader.stats.sort", sort);

	  FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
	  job.setInputFormat(SequenceFileInputFormat.class);

	  job.setMapperClass(CrawlDbStatMapper.class);
	  job.setCombinerClass(CrawlDbStatCombiner.class);
	  job.setReducerClass(CrawlDbStatReducer.class);

	  FileOutputFormat.setOutputPath(job, tmpFolder);
	  job.setOutputFormat(SequenceFileOutputFormat.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(NutchWritable.class);

	  // https://issues.apache.org/jira/browse/NUTCH-1029
	  job.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

	  JobClient.runJob(job);

	  // reading the result
	  FileSystem fileSystem = tmpFolder.getFileSystem(config);
	  SequenceFile.Reader[] readers = SequenceFileOutputFormat.getReaders(config,
			  tmpFolder);

	  Text key = new Text();
	  NutchWritable value = new NutchWritable();

	  TreeMap<String, Writable> stats = new TreeMap<>();
	  for (int i = 0; i < readers.length; i++) {
		  SequenceFile.Reader reader = readers[i];
		  while (reader.next(key, value)) {
			  String k = key.toString();
			  Writable val = stats.get(k);
			  if (val == null) {
			    stats.put(k, value.get());
			    continue;
			  }
			  if (k.equals("scx")) {
			    FloatWritable fvalue = (FloatWritable) value.get();
			    if (((FloatWritable) val).get() < fvalue.get())
			      ((FloatWritable) val).set(fvalue.get());
        } else if (k.equals("ftx") || k.equals("fix")) {
          LongWritable lvalue = (LongWritable) value.get();
          if (((LongWritable) val).get() < lvalue.get())
            ((LongWritable) val).set(lvalue.get());
        } else if (k.equals("scn")) {
          FloatWritable fvalue = (FloatWritable) value.get();
          if (((FloatWritable) val).get() > fvalue.get())
            ((FloatWritable) val).set(fvalue.get());
			  } else if (k.equals("ftn") || k.equals("fin")) {
          LongWritable lvalue = (LongWritable) value.get();
				  if (((LongWritable) val).get() > lvalue.get())
				    ((LongWritable) val).set(lvalue.get());
			  } else if (k.equals("sct")) {
          FloatWritable fvalue = (FloatWritable) value.get();
          ((FloatWritable) val)
              .set(((FloatWritable) val).get() + fvalue.get());
        } else if (k.equals("scd") || k.equals("ftd") || k.equals("fid")) {
          MergingDigest tdigest = null;
          MergingDigest tdig = MergingDigest.fromBytes(
              ByteBuffer.wrap(((BytesWritable) value.get()).getBytes()));
          if (val instanceof BytesWritable) {
            tdigest = MergingDigest.fromBytes(
                ByteBuffer.wrap(((BytesWritable) val).getBytes()));
            tdigest.add(tdig);
          } else {
            tdigest = tdig;
          }
          ByteBuffer tdigestBytes = ByteBuffer
              .allocate(tdigest.smallByteSize());
          tdigest.asSmallBytes(tdigestBytes);
          stats.put(k, new BytesWritable(tdigestBytes.array()));
        } else {
          LongWritable lvalue = (LongWritable) value.get();
          ((LongWritable) val)
              .set(((LongWritable) val).get() + lvalue.get());
			  }
		  }
		  reader.close();
	  }
	  // removing the tmp folder
	  fileSystem.delete(tmpFolder, true);
	  return stats;
  }
  
  public void processStatJob(String crawlDb, Configuration config, boolean sort)
      throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb statistics start: " + crawlDb);
    }
    TreeMap<String, Writable> stats = processStatJobHelper(crawlDb, config, sort);

    if (LOG.isInfoEnabled()) {
      LOG.info("Statistics for CrawlDb: " + crawlDb);
      LongWritable totalCnt = ((LongWritable) stats.get("T"));
      stats.remove("T");
      LOG.info("TOTAL urls:\t" + totalCnt.get());
      for (Map.Entry<String, Writable> entry : stats.entrySet()) {
        String k = entry.getKey();
        long value = 0;
        double fvalue = 0.0;
        byte[] bytesValue = null;
        Writable val = entry.getValue();
        if (val instanceof LongWritable) {
          value = ((LongWritable) val).get();
        } else if (val instanceof FloatWritable) {
          fvalue = ((FloatWritable) val).get();
        } else if (val instanceof BytesWritable) {
          bytesValue = ((BytesWritable) val).getBytes();
        }
        if (k.equals("scn")) {
          LOG.info("min score:\t" + fvalue);
        } else if (k.equals("scx")) {
          LOG.info("max score:\t" + fvalue);
        } else if (k.equals("sct")) {
          LOG.info("avg score:\t" + (fvalue / totalCnt.get()));
        } else if (k.equals("ftn")) {
          LOG.info("earliest fetch time:\t" + new Date(1000 * 60 * value));
        } else if (k.equals("ftx")) {
          LOG.info("latest fetch time:\t" + new Date(1000 * 60 * value));
        } else if (k.equals("ftt")) {
          LOG.info("avg of fetch times:\t"
              + new Date(1000 * 60 * (value / totalCnt.get())));
        } else if (k.equals("fin")) {
          LOG.info("shortest fetch interval:\t{}",
              TimingUtil.secondsToDaysHMS(value));
        } else if (k.equals("fix")) {
          LOG.info("longest fetch interval:\t{}",
              TimingUtil.secondsToDaysHMS(value));
        } else if (k.equals("fit")) {
          LOG.info("avg fetch interval:\t{}",
              TimingUtil.secondsToDaysHMS(value / totalCnt.get()));
        } else if (k.startsWith("status")) {
          String[] st = k.split(" ");
          int code = Integer.parseInt(st[1]);
          if (st.length > 2)
            LOG.info("   " + st[2] + " :\t" + val);
          else
            LOG.info(st[0] + " " + code + " ("
                + CrawlDatum.getStatusName((byte) code) + "):\t" + val);
        } else if (k.equals("scd")) {
          MergingDigest tdigest = MergingDigest
              .fromBytes(ByteBuffer.wrap(bytesValue));
          if (k.startsWith("sc")) {
            double quantiles[] = { .01, .05, .1, .2, .25, .3, .4, .5, .6, .7,
                .75, .8, .9, .95, .99 };
            for (double q : quantiles) {
              LOG.info("score quantile {}:\t{}", q, tdigest.quantile(q));
            }
          }
        } else {
          LOG.info(k + ":\t" + val);
        }
      }
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb statistics: done");
    }

  }

  public CrawlDatum get(String crawlDb, String url, JobConf config)
      throws IOException {
    Text key = new Text(url);
    CrawlDatum val = new CrawlDatum();
    openReaders(crawlDb, config);
    CrawlDatum res = (CrawlDatum) MapFileOutputFormat.getEntry(readers,
        new HashPartitioner<>(), key, val);
    return res;
  }

  public void readUrl(String crawlDb, String url, JobConf config)
      throws IOException {
    CrawlDatum res = get(crawlDb, url, config);
    System.out.println("URL: " + url);
    if (res != null) {
      System.out.println(res);
    } else {
      System.out.println("not found");
    }
  }

  public void processDumpJob(String crawlDb, String output,
      JobConf config, String format, String regex, String status,
      Integer retry, String expr, Float sample) throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb dump: starting");
      LOG.info("CrawlDb db: " + crawlDb);
    }

    Path outFolder = new Path(output);

    JobConf job = new NutchJob(config);
    job.setJobName("dump " + crawlDb);

    FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    FileOutputFormat.setOutputPath(job, outFolder);

    if (format.equals("csv")) {
      job.setOutputFormat(CrawlDatumCsvOutputFormat.class);
    } else if (format.equals("crawldb")) {
      job.setOutputFormat(MapFileOutputFormat.class);
    } else {
      job.setOutputFormat(TextOutputFormat.class);
    }

    if (status != null)
      job.set("status", status);
    if (regex != null)
      job.set("regex", regex);
    if (retry != null)
      job.setInt("retry", retry);
    if (expr != null) {
      job.set("expr", expr);
      LOG.info("CrawlDb db: expr: " + expr);
    }
    if (sample != null)
      job.setFloat("sample", sample);

    job.setMapperClass(CrawlDbDumpMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    JobClient.runJob(job);
    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb dump: done");
    }
  }

  public static class CrawlDbDumpMapper implements
      Mapper<Text, CrawlDatum, Text, CrawlDatum> {
    Pattern pattern = null;
    Matcher matcher = null;
    String status = null;
    Integer retry = null;
    Expression expr = null;
    float sample;

    public void configure(JobConf job) {
      if (job.get("regex", null) != null) {
        pattern = Pattern.compile(job.get("regex"));
      }
      status = job.get("status", null);
      retry = job.getInt("retry", -1);
      
      if (job.get("expr", null) != null) {
        expr = JexlUtil.parseExpression(job.get("expr", null));
      }
      sample = job.getFloat("sample", 1);
    }

    public void close() {
    }

    public void map(Text key, CrawlDatum value,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {

      // check sample
      if (sample < 1 && Math.random() > sample) {
        return;
      }
      // check retry
      if (retry != -1) {
        if (value.getRetriesSinceFetch() < retry) {
          return;
        }
      }

      // check status
      if (status != null
          && !status.equalsIgnoreCase(CrawlDatum.getStatusName(value
              .getStatus())))
        return;

      // check regex
      if (pattern != null) {
        matcher = pattern.matcher(key.toString());
        if (!matcher.matches()) {
          return;
        }
      }
      
      // check expr
      if (expr != null) {
        if (!value.evaluate(expr)) {
          return;
        }
      }

      output.collect(key, value);
    }
  }

  public void processTopNJob(String crawlDb, long topN, float min,
      String output, JobConf config) throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb topN: starting (topN=" + topN + ", min=" + min + ")");
      LOG.info("CrawlDb db: " + crawlDb);
    }

    Path outFolder = new Path(output);
    Path tempDir = new Path(config.get("mapred.temp.dir", ".")
        + "/readdb-topN-temp-"
        + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.setJobName("topN prepare " + crawlDb);
    FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(CrawlDbTopNMapper.class);
    job.setReducerClass(IdentityReducer.class);

    FileOutputFormat.setOutputPath(job, tempDir);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);

    job.setFloat("db.reader.topn.min", min);
    JobClient.runJob(job);

    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb topN: collecting topN scores.");
    }
    job = new NutchJob(config);
    job.setJobName("topN collect " + crawlDb);
    job.setLong("db.reader.topn", topN);

    FileInputFormat.addInputPath(job, tempDir);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(IdentityMapper.class);
    job.setReducerClass(CrawlDbTopNReducer.class);

    FileOutputFormat.setOutputPath(job, outFolder);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1); // create a single file.

    JobClient.runJob(job);
    FileSystem fs = tempDir.getFileSystem(config);
    fs.delete(tempDir, true);
    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb topN: done");
    }

  }

  public int run(String[] args) throws IOException {
    @SuppressWarnings("resource")
    CrawlDbReader dbr = new CrawlDbReader();

    if (args.length < 2) {
      System.err
          .println("Usage: CrawlDbReader <crawldb> (-stats | -dump <out_dir> | -topN <nnnn> <out_dir> [<min>] | -url <url>)");
      System.err
          .println("\t<crawldb>\tdirectory name where crawldb is located");
      System.err
          .println("\t-stats [-sort] \tprint overall statistics to System.out");
      System.err.println("\t\t[-sort]\tlist status sorted by host");
      System.err
          .println("\t-dump <out_dir> [-format normal|csv|crawldb]\tdump the whole db to a text file in <out_dir>");
      System.err.println("\t\t[-format csv]\tdump in Csv format");
      System.err
          .println("\t\t[-format normal]\tdump in standard format (default option)");
      System.err.println("\t\t[-format crawldb]\tdump as CrawlDB");
      System.err.println("\t\t[-regex <expr>]\tfilter records with expression");
      System.err.println("\t\t[-retry <num>]\tminimum retry count");
      System.err
          .println("\t\t[-status <status>]\tfilter records by CrawlDatum status");
      System.err.println("\t\t[-expr <expr>]\tJexl expression to evaluate for this record");
      System.err.println("\t\t[-sample <fraction>]\tOnly process a random sample with this ratio");
      System.err
          .println("\t-url <url>\tprint information on <url> to System.out");
      System.err
          .println("\t-topN <nnnn> <out_dir> [<min>]\tdump top <nnnn> urls sorted by score to <out_dir>");
      System.err
          .println("\t\t[<min>]\tskip records with scores below this value.");
      System.err.println("\t\t\tThis can significantly improve performance.");
      return -1;
    }
    String param = null;
    String crawlDb = args[0];
    JobConf job = new NutchJob(getConf());
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-stats")) {
        boolean toSort = false;
        if (i < args.length - 1 && "-sort".equals(args[i + 1])) {
          toSort = true;
          i++;
        }
        dbr.processStatJob(crawlDb, job, toSort);
      } else if (args[i].equals("-dump")) {
        param = args[++i];
        String format = "normal";
        String regex = null;
        Integer retry = null;
        String status = null;
        String expr = null;
        Float sample = null;
        for (int j = i + 1; j < args.length; j++) {
          if (args[j].equals("-format")) {
            format = args[++j];
            i = i + 2;
          }
          if (args[j].equals("-regex")) {
            regex = args[++j];
            i = i + 2;
          }
          if (args[j].equals("-retry")) {
            retry = Integer.parseInt(args[++j]);
            i = i + 2;
          }
          if (args[j].equals("-status")) {
            status = args[++j];
            i = i + 2;
          }
          if (args[j].equals("-expr")) {
            expr = args[++j];
            i=i+2;
          }
          if (args[j].equals("-sample")) {
            sample = Float.parseFloat(args[++j]);
            i = i + 2;
          }
        }
        dbr.processDumpJob(crawlDb, param, job, format, regex, status, retry, expr, sample);
      } else if (args[i].equals("-url")) {
        param = args[++i];
        dbr.readUrl(crawlDb, param, job);
      } else if (args[i].equals("-topN")) {
        param = args[++i];
        long topN = Long.parseLong(param);
        param = args[++i];
        float min = 0.0f;
        if (i < args.length - 1) {
          min = Float.parseFloat(args[++i]);
        }
        dbr.processTopNJob(crawlDb, topN, min, param, job);
      } else {
        System.err.println("\nError: wrong argument " + args[i]);
        return -1;
      }
    }
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(NutchConfiguration.create(),
        new CrawlDbReader(), args);
    System.exit(result);
  }

  public Object query(Map<String, String> args, Configuration conf, String type, String crawlId) throws Exception {

    Map<String, Object> results = new HashMap<>();
    String crawlDb = crawlId + "/crawldb";

    if(type.equalsIgnoreCase("stats")){
      boolean sort = false;
      if(args.containsKey("sort")){
        if(args.get("sort").equalsIgnoreCase("true"))
          sort = true;
      }
      TreeMap<String , Writable> stats = processStatJobHelper(crawlDb, NutchConfiguration.create(), sort);
      LongWritable totalCnt = (LongWritable) stats.get("T");
      stats.remove("T");
      results.put("totalUrls", String.valueOf(totalCnt.get()));
      Map<String, Object> statusMap = new HashMap<>();

      for (Map.Entry<String, Writable> entry : stats.entrySet()) {
        String k = entry.getKey();
        long val = 0L;
        double fval = 0.0;
        if (entry.getValue() instanceof LongWritable) {
          val = ((LongWritable) entry.getValue()).get();
        } else if (entry.getValue() instanceof FloatWritable) {
          fval = ((FloatWritable) entry.getValue()).get();
        } else if (entry.getValue() instanceof BytesWritable) {
          continue;
        }
        if (k.equals("scn")) {
          results.put("minScore", String.valueOf(fval));
        } else if (k.equals("scx")) {
          results.put("maxScore", String.valueOf(fval));
        } else if (k.equals("sct")) {
          results.put("avgScore", String.valueOf((fval / totalCnt.get())));
        } else if (k.startsWith("status")) {
          String[] st = k.split(" ");
          int code = Integer.parseInt(st[1]);
          if (st.length > 2){
            @SuppressWarnings("unchecked")
            Map<String, Object> individualStatusInfo = (Map<String, Object>) statusMap.get(String.valueOf(code));
            Map<String, String> hostValues;
            if(individualStatusInfo.containsKey("hostValues")){
              hostValues= (Map<String, String>) individualStatusInfo.get("hostValues");
            }
            else{
              hostValues = new HashMap<>();
              individualStatusInfo.put("hostValues", hostValues);
            }
            hostValues.put(st[2], String.valueOf(val));
          } else {
            Map<String, Object> individualStatusInfo = new HashMap<>();

            individualStatusInfo.put("statusValue", CrawlDatum.getStatusName((byte) code));
            individualStatusInfo.put("count", String.valueOf(val));

            statusMap.put(String.valueOf(code), individualStatusInfo);
          }
        } else {
          results.put(k, String.valueOf(val));
        }
      }
      results.put("status", statusMap);
      return results;
    }
    if(type.equalsIgnoreCase("dump")){
      String output = args.get("out_dir");
      String format = "normal";
      String regex = null;
      Integer retry = null;
      String status = null;
      String expr = null;
      Float sample = null;
      if (args.containsKey("format")) {
        format = args.get("format");
      }
      if (args.containsKey("regex")) {
        regex = args.get("regex");
      }
      if (args.containsKey("retry")) {
        retry = Integer.parseInt(args.get("retry"));
      }
      if (args.containsKey("status")) {
        status = args.get("status");
      }
      if (args.containsKey("expr")) {
        expr = args.get("expr");
      }
      if (args.containsKey("sample")) {
    	  sample = Float.parseFloat(args.get("sample"));
        }
      processDumpJob(crawlDb, output, new NutchJob(conf), format, regex, status, retry, expr, sample);
      File dumpFile = new File(output+"/part-00000");
      return dumpFile;		  
    }
    if (type.equalsIgnoreCase("topN")) {
      String output = args.get("out_dir");
      long topN = Long.parseLong(args.get("nnn"));
      float min = 0.0f;
      if(args.containsKey("min")){
        min = Float.parseFloat(args.get("min"));
      }
      processTopNJob(crawlDb, topN, min, output, new NutchJob(conf));
      File dumpFile = new File(output+"/part-00000");
      return dumpFile;
    }

    if(type.equalsIgnoreCase("url")){
      String url = args.get("url");
      CrawlDatum res = get(crawlDb, url, new NutchJob(conf));
      results.put("status", res.getStatus());
      results.put("fetchTime", new Date(res.getFetchTime()));
      results.put("modifiedTime", new Date(res.getModifiedTime()));
      results.put("retriesSinceFetch", res.getRetriesSinceFetch());
      results.put("retryInterval", res.getFetchInterval());
      results.put("score", res.getScore());
      results.put("signature", StringUtil.toHexString(res.getSignature()));
      Map<String, String> metadata = new HashMap<>();
      if(res.getMetaData()!=null){
        for (Entry<Writable, Writable> e : res.getMetaData().entrySet()) {
          metadata.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
        }
      }
      results.put("metadata", metadata);

      return results;
    }
    return results;
  }
}

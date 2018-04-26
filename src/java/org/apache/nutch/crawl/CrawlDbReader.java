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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.TreeMap;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.util.AbstractChecker;
import org.apache.nutch.util.JexlUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.SegmentReaderUtil;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TimingUtil;
import org.apache.commons.jexl2.Expression;

/**
 * Read utility for the CrawlDB.
 * 
 * @author Andrzej Bialecki
 * 
 */
public class CrawlDbReader extends AbstractChecker implements Closeable {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private MapFile.Reader[] readers = null;

  protected String crawlDb;

  private void openReaders(String crawlDb, Configuration config)
      throws IOException {
    if (readers != null)
      return;
    Path crawlDbPath = new Path(crawlDb, CrawlDb.CURRENT_NAME);
    readers = MapFileOutputFormat.getReaders(crawlDbPath, config);
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
    readers = null;
  }

  public static class CrawlDatumCsvOutputFormat extends
      FileOutputFormat<Text, CrawlDatum> {
    protected static class LineRecordWriter extends
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

      public synchronized void close(TaskAttemptContext context) throws IOException {
        out.close();
      }
    }

    public RecordWriter<Text, CrawlDatum> getRecordWriter(TaskAttemptContext
        context) throws IOException {
      String name = getUniqueFile(context, "part", "");
      Path dir = FileOutputFormat.getOutputPath(context);
      FileSystem fs = dir.getFileSystem(context.getConfiguration());
      DataOutputStream fileOut = fs.create(new Path(dir, name), context);
      return new LineRecordWriter(fileOut);
    }
  }

  public static class CrawlDbStatMapper extends
      Mapper<Text, CrawlDatum, Text, NutchWritable> {
    NutchWritable COUNT_1 = new NutchWritable(new LongWritable(1));
    private boolean sort = false;

    public void setup(Mapper<Text, CrawlDatum, Text, NutchWritable>.Context context) {
      Configuration conf = context.getConfiguration();
      sort = conf.getBoolean("db.reader.stats.sort", false);
    }

    public void close() {
    }

    public void map(Text key, CrawlDatum value, Context context)
        throws IOException, InterruptedException {
      context.write(new Text("T"), COUNT_1);
      context.write(new Text("status " + value.getStatus()), COUNT_1);
      context.write(new Text("retry " + value.getRetriesSinceFetch()), 
          COUNT_1);

      if (Float.isNaN(value.getScore())) {
        context.write(new Text("scNaN"), COUNT_1);
      } else {
        NutchWritable score = new NutchWritable(
            new FloatWritable(value.getScore()));
        context.write(new Text("sc"), score);
        context.write(new Text("sct"), score);
        context.write(new Text("scd"), score);
      }

      // fetch time (in minutes to prevent from overflows when summing up)
      NutchWritable fetchTime = new NutchWritable(
          new LongWritable(value.getFetchTime() / (1000 * 60)));
      context.write(new Text("ft"), fetchTime);
      context.write(new Text("ftt"), fetchTime);

      // fetch interval (in seconds)
      NutchWritable fetchInterval = new NutchWritable(new LongWritable(value.getFetchInterval()));
      context.write(new Text("fi"), fetchInterval);
      context.write(new Text("fit"), fetchInterval);

      if (sort) {
        URL u = new URL(key.toString());
        String host = u.getHost();
        context.write(new Text("status " + value.getStatus() + " " + host),
            COUNT_1);
      }
    }
  }

  public static class CrawlDbStatReducer extends
      Reducer<Text, NutchWritable, Text, NutchWritable> {
    public void setup(Reducer<Text, NutchWritable, Text, NutchWritable>.Context context) {
    }

    public void close() {
    }

    public void reduce(Text key, Iterable<NutchWritable> values,
        Context context)
        throws IOException, InterruptedException {
      String k = key.toString();
      if (k.equals("T") || k.startsWith("status") || k.startsWith("retry")
          || k.equals("ftt") || k.equals("fit")) {
        // sum all values for this key
        long sum = 0;
        for (NutchWritable value : values) {
          sum += ((LongWritable) value.get()).get();
        }
        // output sum
        context.write(key, new NutchWritable(new LongWritable(sum)));
      } else if (k.equals("sc")) {
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        for (NutchWritable nvalue : values) {
          float value = ((FloatWritable) nvalue.get()).get();
          if (max < value) {
            max = value;
          }
          if (min > value) {
            min = value;
          }
        }
        context.write(key, new NutchWritable(new FloatWritable(min)));
        context.write(key, new NutchWritable(new FloatWritable(max)));
      } else if (k.equals("ft") || k.equals("fi")) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (NutchWritable nvalue : values) {
          long value = ((LongWritable) nvalue.get()).get();
          if (max < value) {
            max = value;
          }
          if (min > value) {
            min = value;
          }
        }
        context.write(key, new NutchWritable(new LongWritable(min)));
        context.write(key, new NutchWritable(new LongWritable(max)));
      } else if (k.equals("sct")) {
        float cnt = 0.0f;
        for (NutchWritable nvalue : values) {
          float value = ((FloatWritable) nvalue.get()).get();
          cnt += value;
        }
        context.write(key, new NutchWritable(new FloatWritable(cnt)));
      } else if (k.equals("scd")) {
        MergingDigest tdigest = null;
        for (NutchWritable nvalue : values) {
          Writable value = nvalue.get();
          if (value instanceof BytesWritable) {
            byte[] bytes = ((BytesWritable) value).getBytes();
            MergingDigest tdig = MergingDigest
                .fromBytes(ByteBuffer.wrap(bytes));
            if (tdigest == null) {
              tdigest = tdig;
            } else {
              tdigest.add(tdig);
            }
          } else if (value instanceof FloatWritable) {
            float val = ((FloatWritable) value).get();
            if (!Float.isNaN(val)) {
              if (tdigest == null) {
                tdigest = (MergingDigest) TDigest.createMergingDigest(100.0);
              }
              tdigest.add(val);
            }
          }
        }
        ByteBuffer tdigestBytes = ByteBuffer.allocate(tdigest.smallByteSize());
        tdigest.asSmallBytes(tdigestBytes);
        context.write(key,
            new NutchWritable(new BytesWritable(tdigestBytes.array())));
      }
    }
  }

  public static class CrawlDbTopNMapper extends
      Mapper<Text, CrawlDatum, FloatWritable, Text> {
    private static final FloatWritable fw = new FloatWritable();
    private float min = 0.0f;

    public void setup(Mapper<Text, CrawlDatum, FloatWritable, Text>.Context context) {
      Configuration conf = context.getConfiguration();
      min = conf.getFloat("db.reader.topn.min", 0.0f);
    }

    public void close() {
    }

    public void map(Text key, CrawlDatum value,
        Context context)
        throws IOException, InterruptedException {
      if (value.getScore() < min)
        return; // don't collect low-scoring records
      fw.set(-value.getScore()); // reverse sorting order
      context.write(fw, key); // invert mapping: score -> url
    }
  }

  public static class CrawlDbTopNReducer extends
      Reducer<FloatWritable, Text, FloatWritable, Text> {
    private long topN;
    private long count = 0L;

    public void reduce(FloatWritable key, Iterable<Text> values,
        Context context)
        throws IOException, InterruptedException {
      for (Text value : values) {
        if (count < topN) {
          key.set(-key.get());
          context.write(key, value);
          count++;
        }
      }
    }

    public void setup(Reducer<FloatWritable, Text, FloatWritable, Text>.Context context) {
      Configuration conf = context.getConfiguration();
      topN = conf.getLong("db.reader.topn", 100) / Integer.parseInt(conf.get("mapreduce.job.reduces"));
    }

    public void close() {
    }
  }

  public void close() {
    closeReaders();
  }

  private TreeMap<String, Writable> processStatJobHelper(String crawlDb, Configuration config, boolean sort) 
          throws IOException, InterruptedException, ClassNotFoundException{
	  Path tmpFolder = new Path(crawlDb, "stat_tmp" + System.currentTimeMillis());

	  Job job = NutchJob.getInstance(config);
          config = job.getConfiguration();
	  job.setJobName("stats " + crawlDb);
	  config.setBoolean("db.reader.stats.sort", sort);

	  FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
	  job.setInputFormatClass(SequenceFileInputFormat.class);

	  job.setJarByClass(CrawlDbReader.class);
	  job.setMapperClass(CrawlDbStatMapper.class);
	  job.setCombinerClass(CrawlDbStatReducer.class);
	  job.setReducerClass(CrawlDbStatReducer.class);

	  FileOutputFormat.setOutputPath(job, tmpFolder);
	  job.setOutputFormatClass(SequenceFileOutputFormat.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(NutchWritable.class);

	  // https://issues.apache.org/jira/browse/NUTCH-1029
	  config.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
          FileSystem fileSystem = tmpFolder.getFileSystem(config);
          try {
            boolean success = job.waitForCompletion(true);
            if (!success) {
              String message = "CrawlDbReader job did not succeed, job status:"
                  + job.getStatus().getState() + ", reason: "
                  + job.getStatus().getFailureInfo();
              LOG.error(message);
              fileSystem.delete(tmpFolder, true);
              throw new RuntimeException(message);
            }
          } catch (IOException | InterruptedException | ClassNotFoundException e) {
            LOG.error(StringUtils.stringifyException(e));
            fileSystem.delete(tmpFolder, true);
            throw e;
          }
	  // reading the result
          SequenceFile.Reader[] readers = SegmentReaderUtil.getReaders(tmpFolder, config);

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
			  if (k.equals("sc")) {
			    float min = Float.MAX_VALUE;
          float max = Float.MIN_VALUE;
			    if (stats.containsKey("scn")) {
			      min = ((FloatWritable) stats.get("scn")).get();
			    } else {
			      min = ((FloatWritable) stats.get("sc")).get();
			    }
          if (stats.containsKey("scx")) {
            max = ((FloatWritable) stats.get("scx")).get();
          } else {
            max = ((FloatWritable) stats.get("sc")).get();
          }
			    float fvalue = ((FloatWritable) value.get()).get();
			    if (min > fvalue) {
			      min = fvalue;
			    }
          if (max < fvalue) {
            max = fvalue;
          }
          stats.put("scn", new FloatWritable(min));
          stats.put("scx", new FloatWritable(max));
        } else if (k.equals("ft") || k.equals("fi")) {
          long min = Long.MAX_VALUE;
          long max = Long.MIN_VALUE;
          String minKey = k + "n";
          String maxKey = k + "x";
          if (stats.containsKey(minKey)) {
            min = ((LongWritable) stats.get(minKey)).get();
          } else if (stats.containsKey(k)) {
            min = ((LongWritable) stats.get(k)).get();
          }
          if (stats.containsKey(maxKey)) {
            max = ((LongWritable) stats.get(maxKey)).get();
          } else if (stats.containsKey(k)) {
            max = ((LongWritable) stats.get(k)).get();
          }
          long lvalue = ((LongWritable) value.get()).get();
          if (min > lvalue) {
            min = lvalue;
          }
          if (max < lvalue) {
            max = lvalue;
          }
          stats.put(k + "n", new LongWritable(min));
          stats.put(k + "x", new LongWritable(max));
			  } else if (k.equals("sct")) {
          FloatWritable fvalue = (FloatWritable) value.get();
          ((FloatWritable) val)
              .set(((FloatWritable) val).get() + fvalue.get());
        } else if (k.equals("scd")) {
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
    // remove score, fetch interval, and fetch time
    // (used for min/max calculation)
    stats.remove("sc");
    stats.remove("fi");
    stats.remove("ft");
	  // removing the tmp folder
	  fileSystem.delete(tmpFolder, true);
	  return stats;
  }
  
  public void processStatJob(String crawlDb, Configuration config, boolean sort)
      throws IOException, InterruptedException, ClassNotFoundException {

    double quantiles[] = { .01, .05, .1, .2, .25, .3, .4, .5, .6, .7, .75, .8,
        .9, .95, .99 };
    if (config.get("db.stats.score.quantiles") != null) {
      List<Double> qs = new ArrayList<>();
      for (String s : config.getStrings("db.stats.score.quantiles")) {
        try {
          double d = Double.parseDouble(s);
          if (d >= 0.0 && d <= 1.0) {
            qs.add(d);
          } else {
            LOG.warn(
                "Skipping quantile {} not in range in db.stats.score.quantiles: {}",
                s);
          }
        } catch (NumberFormatException e) {
          LOG.warn(
              "Skipping bad floating point number {} in db.stats.score.quantiles: {}",
              s, e.getMessage());
        }
        quantiles = new double[qs.size()];
        int i = 0;
        for (Double q : qs) {
          quantiles[i++] = q;
        }
        Arrays.sort(quantiles);
      }
    }

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
        } else if (k.equals("scNaN")) {
          LOG.info("score == NaN:\t" + value);
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
          for (double q : quantiles) {
            LOG.info("score quantile {}:\t{}", q, tdigest.quantile(q));
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

  public CrawlDatum get(String crawlDb, String url, Configuration config)
      throws IOException {
    Text key = new Text(url);
    CrawlDatum val = new CrawlDatum();
    openReaders(crawlDb, config);
    CrawlDatum res = (CrawlDatum) MapFileOutputFormat.getEntry(readers,
        new HashPartitioner<>(), key, val);
    return res;
  }

  protected int process(String line, StringBuilder output) throws Exception {
    Job job = NutchJob.getInstance(getConf());
    Configuration config = job.getConfiguration();
    // Close readers, so we know we're not working on stale data
    closeReaders();
    readUrl(this.crawlDb, line, config, output);
    return 0;
  }

  public void readUrl(String crawlDb, String url, Configuration config, StringBuilder output)
      throws IOException {
    CrawlDatum res = get(crawlDb, url, config);
    output.append("URL: " + url + "\n");
    if (res != null) {
      output.append(res);
    } else {
      output.append("not found");
    }
    output.append("\n");
  }

  public void processDumpJob(String crawlDb, String output,
      Configuration config, String format, String regex, String status,
      Integer retry, String expr, Float sample) throws IOException, ClassNotFoundException, InterruptedException {
    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb dump: starting");
      LOG.info("CrawlDb db: " + crawlDb);
    }

    Path outFolder = new Path(output);

    Job job = NutchJob.getInstance(config);
    job.setJobName("dump " + crawlDb);

    FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileOutputFormat.setOutputPath(job, outFolder);

    if (format.equals("csv")) {
      job.setOutputFormatClass(CrawlDatumCsvOutputFormat.class);
    } else if (format.equals("crawldb")) {
      job.setOutputFormatClass(MapFileOutputFormat.class);
    } else {
      job.setOutputFormatClass(TextOutputFormat.class);
    }

    if (status != null)
      config.set("status", status);
    if (regex != null)
      config.set("regex", regex);
    if (retry != null)
      config.setInt("retry", retry);
    if (expr != null) {
      config.set("expr", expr);
      LOG.info("CrawlDb db: expr: " + expr);
    }
    if (sample != null)
      config.setFloat("sample", sample);
    job.setMapperClass(CrawlDbDumpMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);
    job.setJarByClass(CrawlDbReader.class);

    try {
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "CrawlDbReader job did not succeed, job status:"
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb dump: done");
    }
  }

  public static class CrawlDbDumpMapper extends
      Mapper<Text, CrawlDatum, Text, CrawlDatum> {
    Pattern pattern = null;
    Matcher matcher = null;
    String status = null;
    Integer retry = null;
    Expression expr = null;
    float sample;

    public void setup(Mapper<Text, CrawlDatum, Text, CrawlDatum>.Context context) {
      Configuration config = context.getConfiguration();
      if (config.get("regex", null) != null) {
        pattern = Pattern.compile(config.get("regex"));
      }
      status = config.get("status", null);
      retry = config.getInt("retry", -1);
      
      if (config.get("expr", null) != null) {
        expr = JexlUtil.parseExpression(config.get("expr", null));
      }
      sample = config.getFloat("sample", 1);
    }

    public void close() {
    }

    public void map(Text key, CrawlDatum value,
        Context context)
        throws IOException, InterruptedException {

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
        if (!value.evaluate(expr, key.toString())) {
          return;
        }
      }

      context.write(key, value);
    }
  }

  public void processTopNJob(String crawlDb, long topN, float min,
      String output, Configuration config) throws IOException, 
      ClassNotFoundException, InterruptedException {

    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb topN: starting (topN=" + topN + ", min=" + min + ")");
      LOG.info("CrawlDb db: " + crawlDb);
    }

    Path outFolder = new Path(output);
    Path tempDir = new Path(config.get("mapreduce.cluster.temp.dir", ".")
        + "/readdb-topN-temp-"
        + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    Job job = NutchJob.getInstance(config);
    job.setJobName("topN prepare " + crawlDb);
    FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setJarByClass(CrawlDbReader.class);
    job.setMapperClass(CrawlDbTopNMapper.class);
    job.setReducerClass(Reducer.class);

    FileOutputFormat.setOutputPath(job, tempDir);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);

    job.getConfiguration().setFloat("db.reader.topn.min", min);
   
    FileSystem fs = tempDir.getFileSystem(config); 
    try{
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "CrawlDbReader job did not succeed, job status:"
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        fs.delete(tempDir, true);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error(StringUtils.stringifyException(e));
      fs.delete(tempDir, true);
      throw e;
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb topN: collecting topN scores.");
    }
    job = NutchJob.getInstance(config);
    job.setJobName("topN collect " + crawlDb);
    job.getConfiguration().setLong("db.reader.topn", topN);

    FileInputFormat.addInputPath(job, tempDir);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(Mapper.class);
    job.setReducerClass(CrawlDbTopNReducer.class);
    job.setJarByClass(CrawlDbReader.class);

    FileOutputFormat.setOutputPath(job, outFolder);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1); // create a single file.

    try{
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "CrawlDbReader job did not succeed, job status:"
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        fs.delete(tempDir, true);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error(StringUtils.stringifyException(e));
      fs.delete(tempDir, true);
      throw e;
    }

    fs.delete(tempDir, true);
    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb topN: done");
    }

  }


  public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException, Exception {
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
    this.crawlDb = crawlDb;
    int numConsumed = 0;
    Job job = NutchJob.getInstance(getConf());
    Configuration config = job.getConfiguration();

    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-stats")) {
        boolean toSort = false;
        if (i < args.length - 1 && "-sort".equals(args[i + 1])) {
          toSort = true;
          i++;
        }
        dbr.processStatJob(crawlDb, config, toSort);
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
        dbr.processDumpJob(crawlDb, param, config, format, regex, status, retry, expr, sample);
      } else if (args[i].equals("-url")) {
        param = args[++i];
        StringBuilder output = new StringBuilder();
        dbr.readUrl(crawlDb, param, config, output);
        System.out.print(output);
      } else if (args[i].equals("-topN")) {
        param = args[++i];
        long topN = Long.parseLong(param);
        param = args[++i];
        float min = 0.0f;
        if (i < args.length - 1) {
          min = Float.parseFloat(args[++i]);
        }
        dbr.processTopNJob(crawlDb, topN, min, param, config);
      } else if ((numConsumed = super.parseArgs(args, i)) > 0) {
        i += numConsumed - 1;
      } else {
        System.err.println("\nError: wrong argument " + args[i]);
        return -1;
      }
    }

    if (numConsumed > 0) {
      // Start listening
      return super.run();
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
      processDumpJob(crawlDb, output, conf, format, regex, status, retry, expr, sample);
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
      processTopNJob(crawlDb, topN, min, output, conf);
      File dumpFile = new File(output+"/part-00000");
      return dumpFile;
    }

    if(type.equalsIgnoreCase("url")){
      String url = args.get("url");
      CrawlDatum res = get(crawlDb, url, conf);
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

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
package org.apache.nutch.hostdb;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.URLUtil;

import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;

/**
 * @see <a href='http://commons.apache.org/proper/commons-jexl/reference/syntax.html'>Commons</a>
 */
public class ReadHostDb extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static final String HOSTDB_DUMP_HEADER = "hostdb.dump.field.header";
  public static final String HOSTDB_DUMP_HOSTNAMES = "hostdb.dump.hostnames";
  public static final String HOSTDB_DUMP_HOMEPAGES = "hostdb.dump.homepages";
  public static final String HOSTDB_FILTER_EXPRESSION = "hostdb.filter.expression";

  static class ReadHostDbMapper extends Mapper<Text, HostDatum, Text, Text> {
    protected boolean dumpHostnames = false;
    protected boolean dumpHomepages = false;
    protected boolean fieldHeader = true;
    protected Text emptyText = new Text();
    protected Expression expr = null;

    public void setup(Context context) {
      dumpHomepages = context.getConfiguration().getBoolean(HOSTDB_DUMP_HOMEPAGES, false);
      dumpHostnames = context.getConfiguration().getBoolean(HOSTDB_DUMP_HOSTNAMES, false);
      fieldHeader = context.getConfiguration().getBoolean(HOSTDB_DUMP_HEADER, true);
      String expr = context.getConfiguration().get(HOSTDB_FILTER_EXPRESSION);
      if (expr != null) {
        // Create or retrieve a JexlEngine
        JexlEngine jexl = new JexlEngine();
        
        // Dont't be silent and be strict
        jexl.setSilent(true);
        jexl.setStrict(true);
        
        // Create an expression object
        this.expr = jexl.createExpression(expr);
      }
    }

    public void map(Text key, HostDatum datum, Context context) throws IOException, InterruptedException {
      if (fieldHeader && !dumpHomepages && !dumpHostnames) {
        context.write(new Text("hostname"), new Text("unfetched\tfetched\tgone\tredirTemp\tredirPerm\tredirSum\tok\tnumRecords\tdnsFail\tcnxFail\tsumFail\tscore\tlastCheck\thomepage\tmetadata"));
        fieldHeader = false;
      }
      
      if (expr != null) {
        // Create a context and add data
        JexlContext jcontext = new MapContext();
        
        // Set some fixed variables
        jcontext.set("unfetched", datum.getUnfetched());
        jcontext.set("fetched", datum.getFetched());
        jcontext.set("gone", datum.getGone());
        jcontext.set("redirTemp", datum.getRedirTemp());
        jcontext.set("redirPerm", datum.getRedirPerm());
        jcontext.set("redirs", datum.getRedirPerm() + datum.getRedirTemp());
        jcontext.set("notModified", datum.getNotModified());
        jcontext.set("ok", datum.getFetched() + datum.getNotModified());
        jcontext.set("numRecords", datum.numRecords());
        jcontext.set("dnsFailures", datum.getDnsFailures());
        jcontext.set("connectionFailures", datum.getConnectionFailures());
        
        // Set metadata variables
        for (Map.Entry<Writable, Writable> entry : datum.getMetaData().entrySet()) {
          Object value = entry.getValue();
          
          if (value instanceof FloatWritable) {
            FloatWritable fvalue = (FloatWritable)value;
            Text tkey = (Text)entry.getKey();
            jcontext.set(tkey.toString(), fvalue.get());
          }
          
          if (value instanceof IntWritable) {
            IntWritable ivalue = (IntWritable)value;
            Text tkey = (Text)entry.getKey();
            jcontext.set(tkey.toString(), ivalue.get());
          }
        }
        
        // Filter this record if evaluation did not pass
        try {
          if (!Boolean.TRUE.equals(expr.evaluate(jcontext))) {
            return;
          }
        } catch (Exception e) {
          LOG.info(e.toString() + " for " + key.toString());
        }
      }
      
      if (dumpHomepages) {
        if (datum.hasHomepageUrl()) {
          context.write(new Text(datum.getHomepageUrl()), emptyText);
        }
        return;
      }
      
      if (dumpHostnames) {
        context.write(key, emptyText);
        return;
      }
      
      // Write anyway
      context.write(key, new Text(datum.toString()));
    }
  }

  // Todo, reduce unknown hosts to single unknown domain if possible. Enable via configuration
  // host_a.example.org,host_a.example.org ==> example.org
//   static class ReadHostDbReducer extends Reduce<Text, Text, Text, Text> {
//     public void setup(Context context) { }
//
//     public void reduce(Text domain, Iterable<Text> hosts, Context context) throws IOException, InterruptedException {
//
//     }
//   }

  private void readHostDb(Path hostDb, Path output, boolean dumpHomepages, boolean dumpHostnames, String expr) throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("ReadHostDb: starting at " + sdf.format(start));

    Configuration conf = getConf();
    conf.setBoolean(HOSTDB_DUMP_HOMEPAGES, dumpHomepages);
    conf.setBoolean(HOSTDB_DUMP_HOSTNAMES, dumpHostnames);
    if (expr != null) {
      conf.set(HOSTDB_FILTER_EXPRESSION, expr);
    }
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
    conf.set("mapred.textoutputformat.separator", "\t");
    
    Job job = Job.getInstance(conf);
    job.setJobName("ReadHostDb");
    job.setJarByClass(ReadHostDb.class);

    FileInputFormat.addInputPath(job, new Path(hostDb, "current"));
    FileOutputFormat.setOutputPath(job, output);

    job.setJarByClass(ReadHostDb.class);
    job.setMapperClass(ReadHostDbMapper.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);

    try {
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "ReadHostDb job did not succeed, job status: "
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        // throw exception so that calling routine can exit with error
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("ReadHostDb job failed", e);
      throw e;
    }

    long end = System.currentTimeMillis();
    LOG.info("ReadHostDb: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
  }
  
  private void getHostDbRecord(Path hostDb, String host) throws Exception {
    Configuration conf = getConf();
    SequenceFile.Reader[] readers = SequenceFileOutputFormat.getReaders(conf, hostDb);

    Class<?> keyClass = readers[0].getKeyClass();
    Class<?> valueClass = readers[0].getValueClass();
    
    if (!keyClass.getName().equals("org.apache.hadoop.io.Text"))
      throw new IOException("Incompatible key (" + keyClass.getName() + ")");
      
    Text key = (Text) keyClass.newInstance();
    HostDatum value = (HostDatum) valueClass.newInstance();
    
    for (int i = 0; i < readers.length; i++) {
      while (readers[i].next(key, value)) {
        if (host.equals(key.toString())) {
          System.out.println(value.toString());
        }
      }
      readers[i].close();
    }    
  }

  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new ReadHostDb(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: ReadHostDb <hostdb> [-get <url>] [<output> [-dumpHomepages | -dumpHostnames | -expr <expr.>]]");
      return -1;
    }

    boolean dumpHomepages = false;
    boolean dumpHostnames = false;
    String expr = null;
    String get = null;

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-dumpHomepages")) {
        LOG.info("ReadHostDb: dumping homepage URL's");
        dumpHomepages = true;
      }
      if (args[i].equals("-dumpHostnames")) {
        LOG.info("ReadHostDb: dumping hostnames");
        dumpHostnames = true;
      }
      if (args[i].equals("-get")) {
        get = args[i + 1];
        LOG.info("ReadHostDb: get: "+ get);
        i++;
      }
      if (args[i].equals("-expr")) {
        expr = args[i + 1];
        LOG.info("ReadHostDb: evaluating expression: " + expr);
        i++;
      }
    }

    try {
      if (get != null) {
        getHostDbRecord(new Path(args[0], "current"), get);
      } else {
        readHostDb(new Path(args[0]), new Path(args[1]), dumpHomepages, dumpHostnames, expr);
      }
      return 0;
    } catch (Exception e) {
      LOG.error("ReadHostDb: " + StringUtils.stringifyException(e));
      return -1;
    }
  }
}

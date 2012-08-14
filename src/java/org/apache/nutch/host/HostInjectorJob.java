/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.host;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.storage.Host;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates or updates an existing host table from a text file.<br>
 * The files contain one host name per line, optionally followed by custom
 * metadata separated by tabs with the metadata key is separated from the
 * corresponding value by '='. <br>
 * The URLs can contain the protocol. It will be stripped if present <br>
 * e.g. http://www.nutch.org \t nutch.score=10 \t nutch.fetchInterval=2592000 \t
 * userType=open_source
 **/

public class HostInjectorJob implements Tool {

  public static final Logger LOG = LoggerFactory
      .getLogger(HostInjectorJob.class);

  private Configuration conf;

  private static final Set<Host.Field> FIELDS = new HashSet<Host.Field>();
  static {
    FIELDS.add(Host.Field.METADATA);
  }

  public HostInjectorJob() {

  }

  public HostInjectorJob(Configuration conf) {
    setConf(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public static class UrlMapper extends
      Mapper<LongWritable, Text, String, Host> {

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String url = value.toString().trim();

      // skip empty lines
      if (url.trim().length() == 0)
        return;

      Map<String, String> metadata = new TreeMap<String, String>();
      if (url.indexOf("\t") != -1) {
        String[] splits = url.split("\t");
        url = splits[0];
        for (int s = 1; s < splits.length; s++) {
          // find separation between name and value
          int indexEquals = splits[s].indexOf("=");
          if (indexEquals == -1) {
            // skip anything without a =
            continue;
          }
          String metaname = splits[s].substring(0, indexEquals).trim();
          String metavalue = splits[s].substring(indexEquals + 1).trim();
          metadata.put(metaname, metavalue);
        }
      }

      // now add the metadata
      Host host = new Host();

      Iterator<String> keysIter = metadata.keySet().iterator();
      while (keysIter.hasNext()) {
        String keymd = keysIter.next();
        String valuemd = metadata.get(keymd);
        host.putToMetadata(new Utf8(keymd), ByteBuffer.wrap(valuemd.getBytes()));
      }
      String hostname;
      if (url.indexOf("://")> -1) {
        hostname=new URL(url).getHost();
      } else {
        hostname=new URL("http://"+url).getHost();
      }
      String hostkey = TableUtil.reverseHost(hostname);
      context.write(hostkey, host);
    }
  }

  public boolean inject(Path hostDir) throws Exception {
    LOG.info("HostInjectorJob: starting");
    LOG.info("HostInjectorJob: hostDir: " + hostDir);
    Job job = new NutchJob(getConf(), "inject-hosts " + hostDir);
    FileInputFormat.addInputPath(job, hostDir);
    job.setMapperClass(UrlMapper.class);
    job.setMapOutputKeyClass(String.class);
    job.setMapOutputValueClass(Host.class);
    job.setOutputFormatClass(GoraOutputFormat.class);
    GoraOutputFormat.setOutput(job,
        StorageUtils.createWebStore(job.getConfiguration(), String.class, Host.class), true);
    job.setReducerClass(Reducer.class);
    job.setNumReduceTasks(0);
    return job.waitForCompletion(true);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: HostInjectorJob <host_dir>");
      return -1;
    }
    try {
      boolean success = inject(new Path(args[0]));
      if (!success) {
        LOG.error("HostInjectorJob: failed ");
        return -1;
      }
      LOG.info("HostInjectorJob: finished");
      return -0;
    } catch (Exception e) {
      LOG.error("HostInjectorJob: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(),
        new HostInjectorJob(), args);
    System.exit(res);
  }
}

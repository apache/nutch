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

import java.lang.invoke.MethodHandles;
import java.io.IOException;
import java.net.UnknownHostException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.MapFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.security.Credentials;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.ContextHandler;
import org.mortbay.jetty.handler.ResourceHandler;

public class CrawlDBTestUtil {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static CrawlDbReducer reducer = new CrawlDbReducer();
  /**
   * Creates synthetic crawldb
   * 
   * @param fs
   *          filesystem where db will be created
   * @param crawldb
   *          path were db will be created
   * @param init
   *          urls to be inserted, objects are of type URLCrawlDatum
   * @throws Exception
   */
  public static void createCrawlDb(Configuration conf, FileSystem fs,
      Path crawldb, List<URLCrawlDatum> init) throws Exception {
    LOG.trace("* creating crawldb: " + crawldb);
    Path dir = new Path(crawldb, CrawlDb.CURRENT_NAME);
    Option wKeyOpt = MapFile.Writer.keyClass(Text.class);
    org.apache.hadoop.io.SequenceFile.Writer.Option wValueOpt = SequenceFile.Writer.valueClass(CrawlDatum.class);
    MapFile.Writer writer = new MapFile.Writer(conf, new Path(dir,
        "part-r-00000"), wKeyOpt, wValueOpt);
    Iterator<URLCrawlDatum> it = init.iterator();
    while (it.hasNext()) {
      URLCrawlDatum row = it.next();
      LOG.info("adding:" + row.url.toString());
      writer.append(new Text(row.url), row.datum);
    }
    writer.close();
  }

  /** {@link Context} to collect all values in a {@link List} */
  private static class DummyContext extends Reducer<Text, CrawlDatum, Text, CrawlDatum>.Context {

    private Configuration conf;

    private DummyContext() {
      reducer.super();
      conf = new Configuration();
    }

    private List<CrawlDatum> values = new ArrayList<CrawlDatum>();

    @Override
    public void write(Text key, CrawlDatum value) throws IOException, InterruptedException {
      values.add(value);
    }

    /** collected values as List */
    public List<CrawlDatum> getValues() {
      return values;
    }

    /** Obtain current collected value from List */
    @Override
    public CrawlDatum getCurrentValue() throws UnsupportedOperationException {
      throw new UnsupportedOperationException("Dummy context");
    }

    /** Obtain current collected key from List */
    @Override
    public Text getCurrentKey() throws UnsupportedOperationException {
      throw new UnsupportedOperationException("Dummy context with no keys");
    }

    private Counters dummyCounters = new Counters();

    public void progress() {
    }

    public Counter getCounter(Enum<?> arg0) {
      return dummyCounters.getGroup("dummy").getCounterForName("dummy");
    }

    public Counter getCounter(String arg0, String arg1) {
      return dummyCounters.getGroup("dummy").getCounterForName("dummy");
    }

    public void setStatus(String arg0) throws UnsupportedOperationException {
      throw new UnsupportedOperationException("Dummy context with no status");
    }

    @Override
    public String getStatus() throws UnsupportedOperationException {
      throw new UnsupportedOperationException("Dummy context with no status");
    }

    public float getProgress() {
      return 1f;
    }

    public OutputCommitter getOutputCommitter() {
      throw new UnsupportedOperationException("Dummy context without committer");
    }

    public boolean nextKey(){
      return false;
    }

    @Override
    public boolean nextKeyValue(){
      return false;
    }

    @Override
    public TaskAttemptID getTaskAttemptID() throws UnsupportedOperationException {
      throw new UnsupportedOperationException("Dummy context without TaskAttemptID");
    }

    @Override
    public Path[] getArchiveClassPaths() {
      return null;
    }

    @Override
    public String[] getArchiveTimestamps() {
      return null;
    }

    @Override
    public URI[] getCacheArchives() throws IOException {
      return null;
    }

    @Override
    public URI[] getCacheFiles() throws IOException {
      return null;
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
      return null;
    }

    @Override
    public RawComparator<?> getCombinerKeyGroupingComparator() {
      return null;
    }
  
    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public Credentials getCredentials() {
      return null;
    }

    @Override
    public Path[] getFileClassPaths() {
      return null;
    }

    @Override
    public String[] getFileTimestamps() {
      return null;
    }

    @Override
    public RawComparator<?> getGroupingComparator() {
      return null;
    }

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
      return null;
    }

    @Override
    public String getJar() {
      return null;
    }

    @Override
    public JobID getJobID() {
      return null;
    }

    @Override
    public String getJobName() {
      return null;
    }

    @Override
    public boolean getJobSetupCleanupNeeded() {
      return false;
    }

    @Override
    @Deprecated
    public Path[] getLocalCacheArchives() throws IOException {
      return null;
    }

    @Override
    @Deprecated
    public Path[] getLocalCacheFiles() throws IOException {
      return null;
    }

    @Override
    public Class<?> getMapOutputKeyClass() {
      return null;
    }

    @Override
    public Class<?> getMapOutputValueClass() {
      return null;
    }

    @Override
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
      return null;
    }

    @Override
    public int getMaxMapAttempts() {
      return 0;
    }

    @Override
    public int getMaxReduceAttempts() {
      return 0;
    }

    @Override
    public int getNumReduceTasks() {
      return 0;
    }

    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
      return null;
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return null;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return null;
    }

    @Override
    public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
      return null;
    }
    
    @Override
    public boolean getProfileEnabled() {
      return false;
    }

    @Override
    public String getProfileParams() {
      return null;
    }

    @Override
    public IntegerRanges getProfileTaskRange(boolean arg0) {
      return null;
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
      return null;
    }

    @Override
    public RawComparator<?> getSortComparator() {
      return null;
    }

    @Override
    @Deprecated
    public boolean getSymlink() {
      return false;
    }

    @Override
    public boolean getTaskCleanupNeeded() {
      return false;
    }

   @Override
    public String getUser() {
      return null;
    }

    @Override
    public Path getWorkingDirectory() throws IOException {
      return null;
    }
  }
  /**
   * For now we need to manually construct our Configuration, because we need to
   * override the default one and it is currently not possible to use
   * dynamically set values.
   * 
   * @return
   */
  public static Reducer<Text, CrawlDatum, Text, CrawlDatum>.Context createContext() {
    DummyContext context = new DummyContext();
    Configuration conf = context.getConfiguration();
    conf.addResource("nutch-default.xml");
    conf.addResource("crawl-tests.xml");
    return (Reducer<Text, CrawlDatum, Text, CrawlDatum>.Context) context;
  }

  public static class URLCrawlDatum {

    public Text url;

    public CrawlDatum datum;

    public URLCrawlDatum(Text url, CrawlDatum datum) {
      this.url = url;
      this.datum = datum;
    }
  }

  /**
   * Generate seedlist
   * 
   * @throws IOException
   */
  public static void generateSeedList(FileSystem fs, Path urlPath,
      List<String> urls) throws IOException {
    generateSeedList(fs, urlPath, urls, new ArrayList<String>());
  }

  /**
   * Generate seedlist
   * 
   * @throws IOException
   */
  public static void generateSeedList(FileSystem fs, Path urlPath,
      List<String> urls, List<String> metadata) throws IOException {
    FSDataOutputStream out;
    Path file = new Path(urlPath, "urls.txt");
    fs.mkdirs(urlPath);
    out = fs.create(file);

    Iterator<String> urls_i = urls.iterator();
    Iterator<String> metadata_i = metadata.iterator();

    String url;
    String md;
    while (urls_i.hasNext()) {
      url = urls_i.next();

      out.writeBytes(url);

      if (metadata_i.hasNext()) {
        md = metadata_i.next();
        out.writeBytes(md);
      }

      out.writeBytes("\n");
    }

    out.flush();
    out.close();
  }

  /**
   * Creates a new JettyServer with one static root context
   * 
   * @param port
   *          port to listen to
   * @param staticContent
   *          folder where static content lives
   * @throws UnknownHostException
   */
  public static Server getServer(int port, String staticContent)
      throws UnknownHostException {
    Server webServer = new org.mortbay.jetty.Server();
    SocketConnector listener = new SocketConnector();
    listener.setPort(port);
    listener.setHost("127.0.0.1");
    webServer.addConnector(listener);
    ContextHandler staticContext = new ContextHandler();
    staticContext.setContextPath("/");
    staticContext.setResourceBase(staticContent);
    staticContext.addHandler(new ResourceHandler());
    webServer.addHandler(staticContext);
    return webServer;
  }
}

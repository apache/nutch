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

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to test transitions of {@link CrawlDatum} states during an update of
 * {@link CrawlDb} (command {@literal updatedb}): call
 * {@link CrawlDbReducer#reduce(Text, Iterator, OutputCollector, Reporter)} with
 * the old CrawlDatum (db status) and the new one (fetch status)
 */
public class CrawlDbUpdateUtil <T extends Reducer<Text, CrawlDatum, Text, CrawlDatum>> {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private CrawlDbReducer reducer;

  public static Text dummyURL = new Text("http://nutch.apache.org/");

  protected CrawlDbUpdateUtil(CrawlDbReducer red, Reducer<Text, CrawlDatum, Text, CrawlDatum>.Context context) throws IOException {
    reducer = red;
    reducer.setup(context);
  }

  /** {@link Context} to collect all values in a {@link List} */
  private class DummyContext extends Reducer<Text, CrawlDatum, Text, CrawlDatum>.Context {
    
    private DummyContext() {
      reducer.super();
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
      return null;
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
   * run
   * {@link CrawlDbReducer#reduce(Text, Iterator, OutputCollector, Reporter)}
   * and return the CrawlDatum(s) which would have been written into CrawlDb
   * 
   * @param values
   *          list of input CrawlDatums
   * @return list of resulting CrawlDatum(s) in CrawlDb
   */
  @SuppressWarnings("unchecked")
  public List<CrawlDatum> update(List<CrawlDatum> values) {
    if (values == null || values.size() == 0) {
      return new ArrayList<CrawlDatum>(0);
    }
    Collections.shuffle(values); // sorting of values should have no influence
    DummyContext context = new DummyContext();
    try {
      Iterable<CrawlDatum> iterable_values = (Iterable)values;
      reducer.reduce(dummyURL, iterable_values, (Reducer<Text, CrawlDatum, Text, CrawlDatum>.Context) context);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    } catch (InterruptedException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    return context.getValues();
  }

  /**
   * run
   * {@link CrawlDbReducer#reduce(Text, Iterator, OutputCollector, Reporter)}
   * and return the CrawlDatum(s) which would have been written into CrawlDb
   * 
   * @param dbDatum
   *          previous CrawlDatum in CrawlDb
   * @param fetchDatum
   *          CrawlDatum resulting from fetching
   * @return list of resulting CrawlDatum(s) in CrawlDb
   */
  public List<CrawlDatum> update(CrawlDatum dbDatum, CrawlDatum fetchDatum) {
    List<CrawlDatum> values = new ArrayList<CrawlDatum>();
    if (dbDatum != null)
      values.add(dbDatum);
    if (fetchDatum != null)
      values.add(fetchDatum);
    return update(values);
  }

  /**
   * see {@link #update(List)}
   */
  public List<CrawlDatum> update(CrawlDatum... values) {
    return update(Arrays.asList(values));
  }

}

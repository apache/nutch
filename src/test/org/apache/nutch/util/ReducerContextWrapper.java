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
package org.apache.nutch.util;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

/**
 * This class wraps an implementation of {@link Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context}, to be used in unit tests,
 *  for example: TestIndexerMapReduce, TestCrawlDbStates.testCrawlDbStatTransitionInject.
 *  
 * @param <KEYIN> Type of input keys
 * @param <VALUEIN> Type of input values 
 * @param <KEYOUT> Type of output keys
 * @param <VALUEOUT> Type of output values
 */
public class ReducerContextWrapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  
  private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer;
  private Configuration config;
  private Counters counters;
  private Map<KEYIN, VALUEIN> valuesIn;
  private Map<KEYOUT, VALUEOUT> valuesOut;
  
  private int valuesIndex;
  private KEYIN currentKey;
  private VALUEIN currentValue;

  private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context;
  
  private String status;

  public ReducerContextWrapper() {
    counters = new Counters();
    valuesIn = new HashMap<>();
    valuesIndex = 0;
  }

  /**
   * Constructs a ReducerContextWrapper
   * 
   * @param reducer The reducer on which to implement the wrapped Reducer.Context
   * @param config The configuration to inject in the wrapped Reducer.Context
   * @param valuesOut The output values to fill (to fake the Hadoop process)
   */
  public ReducerContextWrapper(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer, Configuration config, Map<KEYOUT, VALUEOUT> valuesOut) {
    this();
    this.config = config;
    this.reducer = reducer;
    this.valuesOut = valuesOut;
    initContext();
  }

  /**
   * Return the wrapped Reducer.Context to be used in calls to Reducer.setup and Reducer.reduce, in unit test
   * @return
   */
  public Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context getContext() {
    return context;
  }
  
  private void initContext() {
    // most methods are not used in Nutch unit tests.
    context =  reducer.new Context() {
      
      @Override
      public KEYIN getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
      }

      @Override
      public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        return valuesIndex < valuesIn.size();
      }

      @SuppressWarnings("unchecked")
      @Override
      public void write(Object arg0, Object arg1)
          throws IOException, InterruptedException {
        valuesOut.put((KEYOUT) arg0, (VALUEOUT) arg1);
        currentKey = (KEYIN) arg0;
        currentValue = (VALUEIN) arg1;
        valuesIndex++;
      }

      @Override
      public Counter getCounter(Enum<?> arg0) {
        return counters.findCounter(arg0);
      }

      @Override
      public Counter getCounter(String arg0, String arg1) {
        return counters.findCounter(arg0, arg1);
      }

      @Override
      public float getProgress() {
        return valuesIndex;
      }

      @Override
      public String getStatus() {
        return status;
      }

      @Override
      public void setStatus(String arg0) {
        status = arg0;
      }

      @Override
      public Configuration getConfiguration() {
        return config;
      }

      @Override
      public Iterable<VALUEIN> getValues()
      throws IOException, InterruptedException {
        return valuesIn.values();
      }

      @Override
      public boolean nextKey() throws IOException, InterruptedException {
        return valuesIndex < valuesIn.size();
      }   

      @Override
      public OutputCommitter getOutputCommitter() {
        // Auto-generated
        return null;
      }

      @Override
      public TaskAttemptID getTaskAttemptID() {
        // Auto-generated  
        return null;
      }

      @Override
      public Path[] getArchiveClassPaths() {
        // Auto-generated  
        return null;
      }

      @Override
      public String[] getArchiveTimestamps() {
        // Auto-generated  
        return null;
      }

      @Override
      public URI[] getCacheArchives() throws IOException {
        // Auto-generated  
        return null;
      }

      @Override
      public URI[] getCacheFiles() throws IOException {
        // Auto-generated  
        return null;
      }

      @Override
      public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
          throws ClassNotFoundException {
        // Auto-generated  
        return null;
      }

      @Override
      public RawComparator<?> getCombinerKeyGroupingComparator() {
        // Auto-generated  
        return null;
      }

      @Override
      public Credentials getCredentials() {
        // Auto-generated  
        return null;
      }

      @Override
      public Path[] getFileClassPaths() {
        // Auto-generated  
        return null;
      }

      @Override
      public String[] getFileTimestamps() {
        // Auto-generated  
        return null;
      }

      @Override
      public RawComparator<?> getGroupingComparator() {
        // Auto-generated  
        return null;
      }

      @Override
      public Class<? extends InputFormat<?, ?>> getInputFormatClass()
          throws ClassNotFoundException {
        // Auto-generated  
        return null;
      }

      @Override
      public String getJar() {
        // Auto-generated  
        return null;
      }

      @Override
      public JobID getJobID() {
        // Auto-generated  
        return null;
      }

      @Override
      public String getJobName() {
        // Auto-generated  
        return null;
      }

      @Override
      public boolean getJobSetupCleanupNeeded() {
        // Auto-generated  
        return false;
      }

      @Override
      public Path[] getLocalCacheArchives() throws IOException {
        // Auto-generated  
        return null;
      }

      @Override
      public Path[] getLocalCacheFiles() throws IOException {
        // Auto-generated  
        return null;
      }

      @Override
      public Class<?> getMapOutputKeyClass() {
        // Auto-generated  
        return null;
      }

      @Override
      public Class<?> getMapOutputValueClass() {
        // Auto-generated  
        return null;
      }

      @Override
      public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
          throws ClassNotFoundException {
        // Auto-generated  
        return null;
      }

      @Override
      public int getMaxMapAttempts() {
        // Auto-generated  
        return 0;
      }

      @Override
      public int getMaxReduceAttempts() {
        // Auto-generated  
        return 0;
      }

      @Override
      public int getNumReduceTasks() {
        // Auto-generated  
        return 0;
      }

      @Override
      public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() 
          throws ClassNotFoundException {
        // Auto-generated  
        return null;
      }

      @Override
      public Class<?> getOutputKeyClass() {
        // Auto-generated  
        return null;
      }

      @Override
      public Class<?> getOutputValueClass() {
        // Auto-generated  
        return null;
      }

      @Override
      public Class<? extends Partitioner<?, ?>> getPartitionerClass() 
          throws ClassNotFoundException {
        // Auto-generated  
        return null;
      }

      @Override
      public boolean getProfileEnabled() {
        // Auto-generated  
        return false;
      }

      @Override
      public String getProfileParams() {
        // Auto-generated  
        return null;
      }

      @Override
      public IntegerRanges getProfileTaskRange(boolean arg0) {
        // Auto-generated  
        return null;
      }

      @Override
      public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
      throws ClassNotFoundException {
        // Auto-generated  
        return null;
      }

      @Override
      public RawComparator<?> getSortComparator() {
        // Auto-generated  
        return null;
      }

      @Override
      public boolean getSymlink() {
        // Auto-generated  
        return false;
      }

      @Override
      public boolean getTaskCleanupNeeded() {
        // Auto-generated  
        return false;
      }

      @Override
      public String getUser() {
        // Auto-generated  
        return null;
      }

      @Override
      public Path getWorkingDirectory() throws IOException {
        // Auto-generated  
        return null;
      }

      @Override
      public void progress() {
        // Auto-generated  
      }   
    };
    
  }


}

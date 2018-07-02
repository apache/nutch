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
package org.apache.nutch.tools.arc;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * A input format the reads arc files.
 */
public class ArcInputFormat extends FileInputFormat<Text, BytesWritable> {
  
  public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, 
      TaskAttemptContext context){
    return new SequenceFileRecordReader<Text, BytesWritable>();
  } 
  /**
   * Returns the <code>RecordReader</code> for reading the arc file.
   * 
   * @param split
   *          The InputSplit of the arc file to process.
   * @param job
   *          The job configuration.
   * @param reporter
   *          The progress reporter.
   */
  public RecordReader<Text, BytesWritable> getRecordReader(InputSplit split,
      Job job, Context context) throws IOException {
    context.setStatus(split.toString());
    Configuration conf = job.getConfiguration();
    return new ArcRecordReader(conf, (FileSplit) split);
  }

}

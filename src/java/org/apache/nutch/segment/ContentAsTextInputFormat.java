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
package org.apache.nutch.segment;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.nutch.protocol.Content;

/**
 * An input format that takes Nutch Content objects and converts them to text
 * while converting newline endings to spaces. This format is useful for working
 * with Nutch content objects in Hadoop Streaming with other languages.
 */
public class ContentAsTextInputFormat extends
    SequenceFileInputFormat<Text, Text> {

  private static class ContentAsTextRecordReader extends
      RecordReader<Text, Text> {

    private final SequenceFileRecordReader<Text, Content> sequenceFileRecordReader;

    private Text innerKey;
    private Content innerValue;

    public ContentAsTextRecordReader(Configuration conf, FileSplit split)
        throws IOException {
      sequenceFileRecordReader = new SequenceFileRecordReader<Text, Content>();
      innerKey = new Text();
      innerValue = new Content();
    }

    @Override
    public Text getCurrentValue(){
      return new Text();
    }

    @Override
    public Text getCurrentKey(){
      return new Text();
    }

    @Override
    public boolean nextKeyValue(){
      return false;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context){

    }

    @SuppressWarnings("unused")
    public synchronized boolean next(Text key, Text value) 
        throws IOException, InterruptedException {

      // convert the content object to text
      Text tKey = key;
      if (!sequenceFileRecordReader.nextKeyValue()) {
        return false;
      }
      tKey.set(innerKey.toString());
      String contentAsStr = new String(innerValue.getContent());

      // replace new line endings with spaces
      contentAsStr = contentAsStr.replaceAll("\n", " ");
      value.set(contentAsStr);

      return true;
    }

    @Override
    public float getProgress() throws IOException {
      return sequenceFileRecordReader.getProgress();
    }

    /*public synchronized long getPos() throws IOException {
      return sequenceFileRecordReader.getPos();
    }*/

    @Override
    public synchronized void close() throws IOException {
      sequenceFileRecordReader.close();
    }
  }

  public ContentAsTextInputFormat() {
    super();
  }

  public RecordReader<Text, Text> getRecordReader(InputSplit split,
      Job job, Context context) throws IOException {

    context.setStatus(split.toString());
    Configuration conf = job.getConfiguration();
    return new ContentAsTextRecordReader(conf, (FileSplit) split);
  }
}

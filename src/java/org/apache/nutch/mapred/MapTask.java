/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.mapred;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.nutch.io.*;
import org.apache.nutch.util.NutchConf;
import org.apache.nutch.fs.NutchFileSystem;

/** A Map task. */
public class MapTask extends Task {
  private FileSplit split;

  public MapTask() {}

  public MapTask(String jobFile, String taskId, FileSplit split) {
    super(jobFile, taskId);
    this.split = split;
  }

  public TaskRunner createRunner(TaskTracker tracker) {
    return new MapTaskRunner(this, tracker);
  }

  public FileSplit getSplit() { return split; }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    split.write(out);
    
  }
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    split = new FileSplit();
    split.readFields(in);
  }

  public void run(final JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException {

    // open output files
    final int partitions = job.getNumReduceTasks();
    final SequenceFile.Writer[] outs = new SequenceFile.Writer[partitions];
    try {
      for (int i = 0; i < partitions; i++) {
        outs[i] =
          new SequenceFile.Writer(NutchFileSystem.getNamed("local"),
                                  MapOutputFile.getOutputFile(getTaskId(), i).toString(),
                                  job.getOutputKeyClass(),
                                  job.getOutputValueClass());
      }

      final Partitioner partitioner =
        (Partitioner)job.newInstance(job.getPartitionerClass());

      OutputCollector partCollector = new OutputCollector() { // make collector
          public synchronized void collect(WritableComparable key,
                                           Writable value)
            throws IOException {
            outs[partitioner.getPartition(key, partitions)].append(key, value);
          }
        };

      OutputCollector collector = partCollector;

      boolean combining = job.getCombinerClass() != null;
      if (combining) {                            // add combining collector
        collector = new CombiningCollector(job, partCollector);
      }

      final RecordReader rawIn =                  // open input
        job.getInputFormat().getRecordReader(NutchFileSystem.get(),split,job);

      RecordReader in = new RecordReader() {      // wrap in progress reporter
          private float end = (float)split.getLength();
          private float lastProgress = 0.0f;

          public synchronized boolean next(Writable key, Writable value)
            throws IOException {

            float progress =                        // compute progress
              (float)Math.min((rawIn.getPos()-split.getStart())/end, 1.0f);
            if ((progress - lastProgress) > 0.01f)  { // 100 progress reports
              umbilical.progress(getTaskId(), new FloatWritable(progress));
              lastProgress = progress;
            }

            return rawIn.next(key, value);
          }
          public long getPos() throws IOException { return rawIn.getPos(); }
          public void close() throws IOException { rawIn.close(); }
        };

      MapRunnable runner =
        (MapRunnable)job.newInstance(job.getMapRunnerClass());

      try {
        runner.run(in, collector);                // run the map

        if (combining) {                          // flush combiner
          ((CombiningCollector)collector).flush();
        }

      } finally {
        in.close();                               // close input
      }
    } finally {
      for (int i = 0; i < partitions; i++) {      // close output
        if (outs[i] != null) {
          outs[i].close();
        }
      }
    }
  }
  
}

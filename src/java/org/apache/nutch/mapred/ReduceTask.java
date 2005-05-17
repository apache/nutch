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

import org.apache.nutch.io.*;
import org.apache.nutch.util.*;
import org.apache.nutch.fs.*;

import java.io.*;
import java.net.*;
import java.util.*;

/** A Reduce task. */
public class ReduceTask extends Task {
  private static final String LOCAL_DIR = JobConf.getLocalDir().toString();

  private String[] mapTaskIds;
  private int partition;

  public ReduceTask() {}

  public ReduceTask(String jobFile, String taskId,
                    String[] mapTaskIds, int partition) {
    super(jobFile, taskId);
    this.mapTaskIds = mapTaskIds;
    this.partition = partition;
  }

  public TaskRunner createRunner(TaskTracker tracker) {
    return new ReduceTaskRunner(this, tracker);
  }

  public String[] getMapTaskIds() { return mapTaskIds; }
  public int getPartition() { return partition; }

  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(mapTaskIds.length);              // write mapTaskIds
    for (int i = 0; i < mapTaskIds.length; i++) {
      UTF8.writeString(out, mapTaskIds[i]);
    }

    out.writeInt(partition);                      // write partition
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    mapTaskIds = new String[in.readInt()];        // read mapTaskIds
    for (int i = 0; i < mapTaskIds.length; i++) {
      mapTaskIds[i] = UTF8.readString(in);
    }

    this.partition = in.readInt();                // read partition
  }

  /** Iterates values while keys match in sorted input. */
  private static class ValuesIterator implements Iterator {
    private SequenceFile.Reader in;               // input file
    private WritableComparable key;               // current key
    private Writable value;                       // current value
    private boolean hasNext;                      // more w/ this key
    private boolean more;                         // more in file

    public ValuesIterator (SequenceFile.Reader in) throws IOException {
      this.in = in;
      getNext();
    }

    /// Iterator methods

    public boolean hasNext() { return hasNext; }

    public Object next() {
      try {
        Object result = value;                      // save value
        getNext();                                  // move to next
        return result;                              // return saved value
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public void remove() { throw new RuntimeException("not implemented"); }

    /// Auxiliary methods

    /** Start processing next unique key. */
    public void nextKey() { hasNext = more; }

    /** True iff more keys remain. */
    public boolean more() { return more; }

    /** The current key. */
    public WritableComparable getKey() { return key; }

    private void getNext() throws IOException {
      Writable lastKey = key;                     // save previous key
      try {
        key = (WritableComparable)in.getKeyClass().newInstance();
        value = (Writable)in.getValueClass().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      more = in.next(key, value);
      if (more) {
        if (lastKey == null) {
          hasNext = true;
        } else {
          hasNext = (key.compareTo(lastKey) == 0);
        }
      } else {
        hasNext = false;
      }
    }
  }

  public void run(JobConf job, TaskUmbilicalProtocol umbilical)
    throws IOException {
    Class keyClass = job.getOutputKeyClass();
    Class valueClass = job.getOutputValueClass();
    Reducer reducer = (Reducer)job.newInstance(job.getReducerClass());
        
    umbilical.progress(getTaskId(), new FloatWritable(1.0f/3.0f));

    // open a file to collect map output
    NutchFileSystem lfs = NutchFileSystem.getNamed("local");
    File taskDir = new File(LOCAL_DIR, getTaskId());
    String file = new File(taskDir, "all.in").toString();
    SequenceFile.Writer writer =
      new SequenceFile.Writer(lfs, file, keyClass, valueClass);
    try {
      // append all input files into a single input file
      WritableComparable key;
      Writable value;
      try {
        key = (WritableComparable)keyClass.newInstance();
        value = (Writable)valueClass.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      for (int i = 0; i < mapTaskIds.length; i++) {
        String partFile =
          MapOutputFile.getInputFile(mapTaskIds[i], getTaskId()).toString();
        SequenceFile.Reader in = new SequenceFile.Reader(lfs, partFile);
        try {
          while(in.next(key, value)) {
            writer.append(key, value);
          }
        } finally {
          in.close();
        }
      }
    } finally {
      writer.close();
    }
      
    // sort the input file
    String sortedFile = file+".sorted";
    WritableComparator comparator = null;
    try {
      comparator =
        (WritableComparator)job.getOutputKeyComparatorClass().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    SequenceFile.Sorter sorter =
      new SequenceFile.Sorter(lfs, comparator, valueClass);
    sorter.sort(file, sortedFile);                // sort
    lfs.delete(new File(file));                   // remove unsorted

    umbilical.progress(getTaskId(), new FloatWritable(2.0f/3.0f));

    // make output collector
    String name = "part-" + getPartition();
    final RecordWriter out =
      job.getOutputFormat().getRecordWriter(NutchFileSystem.get(), job, name);
    OutputCollector collector = new OutputCollector() {
        public void collect(WritableComparable key, Writable value)
          throws IOException {
          out.write(key, value);
        }
      };
    
    // apply reduce function
    SequenceFile.Reader in = new SequenceFile.Reader(lfs, sortedFile);
    try {
      ValuesIterator values = new ValuesIterator(in);
      while (values.more()) {
        reducer.reduce(values.getKey(), values, collector);
        values.nextKey();
      }

    } finally {
      in.close();
      lfs.delete(new File(sortedFile));           // remove sorted
      out.close();
    }

    umbilical.progress(getTaskId(), new FloatWritable(3.0f/3.0f));
  }

}

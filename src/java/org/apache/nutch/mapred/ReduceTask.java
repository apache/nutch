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
import java.text.*;

/** A Reduce task. */
public class ReduceTask extends Task {
  private static final String LOCAL_DIR = JobConf.getLocalDir().toString();

  private String[] mapTaskIds;
  private int partition;
  private boolean sortComplete;

  { getProgress().setStatus("reduce"); }

  private Progress copyPhase = getProgress().addPhase("copy");
  private Progress appendPhase = getProgress().addPhase("append");
  private Progress sortPhase  = getProgress().addPhase("sort");
  private Progress reducePhase = getProgress().addPhase("reduce");

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
  private class ValuesIterator implements Iterator {
    private SequenceFile.Reader in;               // input file
    private WritableComparable key;               // current key
    private Writable value;                       // current value
    private boolean hasNext;                      // more w/ this key
    private boolean more;                         // more in file
    private float progPerByte;
    private TaskUmbilicalProtocol umbilical;

    public ValuesIterator (SequenceFile.Reader in, long length,
                           TaskUmbilicalProtocol umbilical)
      throws IOException {
      this.in = in;
      this.progPerByte = 1.0f / (float)length;
      this.umbilical = umbilical;
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
    public void nextKey() {
      while (hasNext) { next(); }                 // skip any unread
      hasNext = more;
    }

    /** True iff more keys remain. */
    public boolean more() { return more; }

    /** The current key. */
    public WritableComparable getKey() { return key; }

    private void getNext() throws IOException {
      reducePhase.set(in.getPosition()*progPerByte); // update progress
      reportProgress(umbilical);

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

  public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException {
    Class keyClass = job.getOutputKeyClass();
    Class valueClass = job.getOutputValueClass();
    Reducer reducer = (Reducer)job.newInstance(job.getReducerClass());
    NutchFileSystem lfs = NutchFileSystem.getNamed("local");

    copyPhase.complete();                         // copy is already complete

    // open a file to collect map output
    File taskDir = new File(LOCAL_DIR, getTaskId());
    String file = new File(taskDir, "all.in").toString();
    SequenceFile.Writer writer =
      new SequenceFile.Writer(lfs, file, keyClass, valueClass);
    try {
      // append all input files into a single input file
      for (int i = 0; i < mapTaskIds.length; i++) {
        appendPhase.addPhase();                 // one per file
      }
      
      DataOutputBuffer buffer = new DataOutputBuffer();

      for (int i = 0; i < mapTaskIds.length; i++) {
        File partFile =
          MapOutputFile.getInputFile(mapTaskIds[i], getTaskId());
        float progPerByte = 1.0f / lfs.getLength(partFile);
        Progress phase = appendPhase.phase();
        phase.setStatus(partFile.toString());
        SequenceFile.Reader in =
          new SequenceFile.Reader(lfs, partFile.toString());
        try {
          int keyLen;
          while((keyLen = in.next(buffer)) > 0) {
            writer.append(buffer.getData(), 0, buffer.getLength(), keyLen);
            phase.set(in.getPosition()*progPerByte);
            reportProgress(umbilical);
            buffer.reset();
          }
        } finally {
          in.close();
        }
        phase.complete();
      }
      
    } finally {
      writer.close();
    }
      
    appendPhase.complete();                     // append is complete

    // spawn a thread to give sort progress heartbeats
    Thread sortProgress = new Thread() {
        public void run() {
          while (!sortComplete) {
            try {
              reportProgress(umbilical);
              Thread.sleep(PROGRESS_INTERVAL);
            } catch (InterruptedException e) {
              continue;
            } catch (Throwable e) {
              return;
            }
          }
        }
      };
    sortProgress.setName("Sort progress reporter for task "+getTaskId());

    String sortedFile = file+".sorted";

    try {
      sortProgress.start();

      // sort the input file
      WritableComparator comparator = 
        (WritableComparator)job.newInstance(job.getOutputKeyComparatorClass());
    
      SequenceFile.Sorter sorter =
        new SequenceFile.Sorter(lfs, comparator, valueClass);
      sorter.sort(file, sortedFile);              // sort
      lfs.delete(new File(file));                 // remove unsorted

    } finally {
      sortComplete = true;
    }

    sortPhase.complete();                         // sort is complete

    // make output collector
    String name = getOutputName(getPartition());
    final RecordWriter out =
      job.getOutputFormat().getRecordWriter(NutchFileSystem.get(), job, name);
    OutputCollector collector = new OutputCollector() {
        public void collect(WritableComparable key, Writable value)
          throws IOException {
          out.write(key, value);
          reportProgress(umbilical);
        }
      };
    
    // apply reduce function
    SequenceFile.Reader in = new SequenceFile.Reader(lfs, sortedFile);
    Reporter reporter = getReporter(umbilical, getProgress());
    long length = lfs.getLength(new File(sortedFile));
    try {
      ValuesIterator values = new ValuesIterator(in, length, umbilical);
      while (values.more()) {
        reducer.reduce(values.getKey(), values, collector, reporter);
        values.nextKey();
      }

    } finally {
      in.close();
      lfs.delete(new File(sortedFile));           // remove sorted
      out.close(reporter);
    }

    umbilical.done(getTaskId());
  }

  /** Construct output file names so that, when an output directory listing is
   * sorted lexicographically, positions correspond to output partitions.*/

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  private static synchronized String getOutputName(int partition) {
    return "part-" + NUMBER_FORMAT.format(partition);
  }

}

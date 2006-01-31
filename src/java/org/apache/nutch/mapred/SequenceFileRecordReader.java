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

import java.io.IOException;
import java.io.File;

import org.apache.nutch.fs.NutchFileSystem;

import org.apache.nutch.io.SequenceFile;
import org.apache.nutch.io.Writable;
import org.apache.nutch.io.WritableComparable;
import org.apache.nutch.io.LongWritable;
import org.apache.nutch.io.UTF8;
import org.apache.nutch.util.NutchConf;

/** An {@link RecordReader} for {@link SequenceFile}s. */
public class SequenceFileRecordReader implements RecordReader {
  private SequenceFile.Reader in;
  private long end;
  private boolean more = true;

  public SequenceFileRecordReader(NutchConf nutchConf, FileSplit split)
    throws IOException {
    NutchFileSystem fs = NutchFileSystem.get(nutchConf);
    this.in = new SequenceFile.Reader(fs, split.getFile().toString(), nutchConf);
    this.end = split.getStart() + split.getLength();

    if (split.getStart() > in.getPosition())
      in.sync(split.getStart());                  // sync to start

    more = in.getPosition() < end;
  }


  /** The class of key that must be passed to {@link
   * #next(Writable,Writable)}.. */
  public Class getKeyClass() { return in.getKeyClass(); }

  /** The class of value that must be passed to {@link
   * #next(Writable,Writable)}.. */
  public Class getValueClass() { return in.getValueClass(); }
  
  public synchronized boolean next(Writable key, Writable value)
    throws IOException {
    if (!more) return false;
    long pos = in.getPosition();
    boolean eof = in.next(key, value);
    if (pos >= end && in.syncSeen()) {
      more = false;
    } else {
      more = eof;
    }
    return more;
  }
  
  public synchronized long getPos() throws IOException {
    return in.getPosition();
  }
  
  public synchronized void close() throws IOException { in.close(); }
  
}


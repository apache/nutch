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

/** An {@link InputFormat} for {@link SequenceFile}s. */
public class SequenceFileInputFormat extends InputFormatBase {

  public RecordReader getRecordReader(NutchFileSystem fs, FileSplit split,
                                      JobConf job) throws IOException {

    // open the file and seek to the start of the split
    final SequenceFile.Reader in =
      new SequenceFile.Reader(fs, split.getFile().toString());
    final long end = split.getStart() + split.getLength();

    in.sync(split.getStart());                    // sync to start

    return new RecordReader() {
        private boolean more = true;

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

      };
  }

}


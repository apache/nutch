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

/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. */
public class SequenceFileInputFormat extends InputFormatBase {

  public String getName() { return "seq"; }

  public RecordReader getRecordReader(NutchFileSystem fs, FileSplit split,
                                      JobConf job) throws IOException {

    // open the file and seek to the start of the split
    final SequenceFile.Reader in =
      new SequenceFile.Reader(fs, split.getFile().toString());
    final long end = split.getStart() + split.getLength();

    in.sync(split.getStart());                    // sync to start

    return new RecordReader() {
        public boolean next(Writable key, Writable value) throws IOException {
          long pos = in.getPosition();
          boolean more = in.next(key, value);
          if (pos >= end && in.syncSeen()) {
            return false;
          } else {
            return more;
          }
        }
        
        public long getPos() throws IOException { return in.getPosition(); }

        public void close() throws IOException { in.close(); }

      };
  }

}


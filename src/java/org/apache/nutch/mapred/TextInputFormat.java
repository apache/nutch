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
import org.apache.nutch.fs.NFSDataInputStream;

import org.apache.nutch.io.Writable;
import org.apache.nutch.io.WritableComparable;
import org.apache.nutch.io.LongWritable;
import org.apache.nutch.io.UTF8;

/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. */
public class TextInputFormat extends InputFormatBase {

  public String getName() { return "text"; }

  public RecordReader getRecordReader(NutchFileSystem fs, FileSplit split,
                                      JobConf job) throws IOException {

    final long start = split.getStart();
    final long end = start + split.getLength();

    // open the file and seek to the start of the split
    final NFSDataInputStream in =
      new NFSDataInputStream(fs.open(split.getFile()));
    in.seek(start);
    
    if (start != 0) {
      while (in.getPos() < end) {    // scan to the next newline in the file
        char c = (char)in.read();
        if (c == '\r' || c == '\n') {
          break;
        }
      }
    }

    return new RecordReader() {
        /** Read a line. */
        public boolean next(Writable key, Writable value) throws IOException {
          long pos = in.getPos();
          if (pos >= end)
            return false;

          ((LongWritable)key).set(pos);           // key is position
          ((UTF8)value).set(readLine(in));        // value is line
          return true;
        }
        
        public long getPos() throws IOException { return in.getPos(); }

        public void close() throws IOException { in.close(); }

      };
  }

  private static String readLine(NFSDataInputStream in) throws IOException {
    StringBuffer buffer = new StringBuffer();
    while (true) {

      int b = in.read();
      if (b == -1)
        break;

      char c = (char)b;              // bug: this assumes eight-bit characters.
      if (c == '\r' || c == '\n')
        break;

      buffer.append(c);
    }
    
    return buffer.toString();
  }

}


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

package org.apache.nutch.io;

import java.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;

/** A dense file-based mapping from integers to values. */
public class ArrayFile extends MapFile {

  protected ArrayFile() {}                            // no public ctor

  /** Write a new array file. */
  public static class Writer extends MapFile.Writer {
    private LongWritable count = new LongWritable(0);

    /** Create the named file for values of the named class. */
    public Writer(NutchFileSystem nfs, String file, Class valClass) throws IOException {
      super(nfs, file, LongWritable.class, valClass);
    }

    /** Append a value to the file. */
    public synchronized void append(Writable value) throws IOException {
      super.append(count, value);                 // add to map
      count.set(count.get()+1);                   // increment count
    }
  }

  /** Provide access to an existing array file. */
  public static class Reader extends MapFile.Reader {
    private LongWritable key = new LongWritable();

    /** Construct an array reader for the named file.*/
    public Reader(NutchFileSystem nfs, String file, NutchConf nutchConf) throws IOException {
      super(nfs, file, nutchConf);
    }

    /** Positions the reader before its <code>n</code>th value. */
    public synchronized void seek(long n) throws IOException {
      key.set(n);
      seek(key);
    }

    /** Read and return the next value in the file. */
    public synchronized Writable next(Writable value) throws IOException {
      return next(key, value) ? value : null;
    }

    /** Returns the key associated with the most recent call to {@link
     * #seek(long)}, {@link #next(Writable)}, or {@link
     * #get(long,Writable)}. */
    public synchronized long key() throws IOException {
      return key.get();
    }

    /** Return the <code>n</code>th value in the file. */
    public synchronized Writable get(long n, Writable value)
      throws IOException {
      key.set(n);
      return get(key, value);
    }
  }

}

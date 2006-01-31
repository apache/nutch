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

/** A file-based set of keys. */
public class SetFile extends MapFile {

  protected SetFile() {}                            // no public ctor

  /** Write a new set file. */
  public static class Writer extends MapFile.Writer {

    /** Create the named set for keys of the named class. */
    public Writer(NutchFileSystem nfs, String dirName, Class keyClass) throws IOException {
      super(nfs, dirName, keyClass, NullWritable.class);
    }

    /** Create the named set using the named key comparator. */
    public Writer(NutchFileSystem nfs, String dirName, WritableComparator comparator)
      throws IOException {
      super(nfs, dirName, comparator, NullWritable.class);
    }

    /** Append a key to a set.  The key must be strictly greater than the
     * previous key added to the set. */
    public void append(WritableComparable key) throws IOException{
      append(key, NullWritable.get());
    }
  }

  /** Provide access to an existing set file. */
  public static class Reader extends MapFile.Reader {

    /** Construct a set reader for the named set.*/
    public Reader(NutchFileSystem nfs, String dirName, NutchConf nutchConf) throws IOException {
      super(nfs, dirName, nutchConf);
    }

    /** Construct a set reader for the named set using the named comparator.*/
    public Reader(NutchFileSystem nfs, String dirName, WritableComparator comparator, NutchConf nutchConf)
      throws IOException {
      super(nfs, dirName, comparator, nutchConf);
    }

    // javadoc inherited
    public boolean seek(WritableComparable key)
      throws IOException {
      return super.seek(key);
    }

    /** Read the next key in a set into <code>key</code>.  Returns
     * true if such a key exists and false when at the end of the set. */
    public boolean next(WritableComparable key)
      throws IOException {
      return next(key, NullWritable.get());
    }

    /** Read the matching key from a set into <code>key</code>.
     * Returns <code>key</code>, or null if no match exists. */
    public WritableComparable get(WritableComparable key)
      throws IOException {
      if (seek(key)) {
        next(key);
        return key;
      } else
        return null;
    }
  }

}

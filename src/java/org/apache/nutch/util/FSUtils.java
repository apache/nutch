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
package org.apache.nutch.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

/**
 * Utility methods for common filesystem operations.
 */
public class FSUtils {

  /**
   * Replaces the current path with the new path and if set removes the old
   * path. If removeOld is set to false then the old path will be set to the
   * name current.old.
   * 
   * @param fs The FileSystem.
   * @param current The end path, the one being replaced.
   * @param replacement The path to replace with.
   * @param removeOld True if we are removing the current path.
   * 
   * @throws IOException If an error occurs during replacement.
   */
  public static void replace(FileSystem fs, Path current, Path replacement,
    boolean removeOld)
    throws IOException {

    // rename any current path to old
    Path old = new Path(current + ".old");
    if (fs.exists(current)) {
      fs.rename(current, old);
    }

    // rename the new path to current and remove the old path if needed
    fs.rename(replacement, current);
    if (fs.exists(old) && removeOld) {
      fs.delete(old, true);
    }
  }

  /**
   * Closes a group of SequenceFile readers.
   * 
   * @param readers The SequenceFile readers to close.
   * @throws IOException If an error occurs while closing a reader.
   */
  public static void closeReaders(SequenceFile.Reader[] readers)
    throws IOException {
    
    // loop through the readers, closing one by one
    if (readers != null) {
      for (int i = 0; i < readers.length; i++) {
        SequenceFile.Reader reader = readers[i];
        if (reader != null) {
          reader.close();
        }
      }
    }
  }

  /**
   * Closes a group of MapFile readers.
   * 
   * @param readers The MapFile readers to close.
   * @throws IOException If an error occurs while closing a reader.
   */
  public static void closeReaders(MapFile.Reader[] readers)
    throws IOException {
    
    // loop through the readers closing one by one
    if (readers != null) {
      for (int i = 0; i < readers.length; i++) {
        MapFile.Reader reader = readers[i];
        if (reader != null) {
          reader.close();
        }
      }
    }
  }
}

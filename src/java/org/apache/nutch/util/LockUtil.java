/**
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

/**
 * Utility methods for handling application-level locking.
 * 
 * @author Andrzej Bialecki
 */
public class LockUtil {
  
  /**
   * Create a lock file.
   * @param fs filesystem
   * @param lockFile name of the lock file
   * @param accept if true, and the target file exists, consider it valid. If false
   * and the target file exists, throw an IOException.
   * @throws IOException if accept is false, and the target file already exists,
   * or if it's a directory.
   */
  public static void createLockFile(FileSystem fs, Path lockFile, boolean accept) throws IOException {
    if (fs.exists(lockFile)) {
      if(!accept)
        throw new IOException("lock file " + lockFile + " already exists.");
      if (fs.getFileStatus(lockFile).isDir())
        throw new IOException("lock file " + lockFile + " already exists and is a directory.");
      // do nothing - the file already exists.
    } else {
      // make sure parents exist
      fs.mkdirs(lockFile.getParent());
      fs.createNewFile(lockFile);
    }
  }

  /**
   * Remove lock file. NOTE: applications enforce the semantics of this file -
   * this method simply removes any file with a given name.
   * @param fs filesystem
   * @param lockFile lock file name
   * @return false, if the lock file doesn't exist. True, if it existed and was
   * successfully removed.
   * @throws IOException if lock file exists but it is a directory.
   */
  public static boolean removeLockFile(FileSystem fs, Path lockFile) throws IOException {
    if (!fs.exists(lockFile)) return false;
    if (fs.getFileStatus(lockFile).isDir())
      throw new IOException("lock file " + lockFile + " exists but is a directory!");
    return fs.delete(lockFile);
  }
}

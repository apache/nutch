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

package org.apache.nutch.mapReduce;

import java.io.IOException;
import java.io.File;

import java.util.ArrayList;

import org.apache.nutch.fs.NutchFileSystem;

/** A base class for {@link InputFormat}. */
public abstract class InputFormatBase implements InputFormat {

  private static final double SPLIT_SLOP = 0.1;   // 10% slop

  public abstract String getName();

  public abstract RecordReader getRecordReader(NutchFileSystem fs,
                                               FileSplit split,
                                               JobConf job) throws IOException;

  /** Subclasses may override to, e.g., select only files matching a regular
   * expression.*/ 
  protected File[] listFiles(NutchFileSystem fs, JobConf job)
    throws IOException {
    File[] files = fs.listFiles(job.getInputDir());
    if (files == null) {
      throw new IOException("No input files in: "+job.getInputDir());
    }
    return files;
  }

  /** Splits files returned by {#listFiles(NutchFileSystem,JobConf) when
   * they're too big.*/ 
  public FileSplit[] getSplits(NutchFileSystem fs, JobConf job, int numSplits)
    throws IOException {

    File[] files = listFiles(fs, job);

    long totalSize = 0;
    for (int i = 0; i < files.length; i++) {
      totalSize += fs.getLength(files[i]);
    }

    long bytesPerSplit = totalSize / numSplits;
    long maxPerSplit = bytesPerSplit + (long)(bytesPerSplit*SPLIT_SLOP);

    ArrayList splits = new ArrayList(numSplits);
    for (int i = 0; i < files.length; i++) {
      File file = files[i];
      long length = fs.getLength(file);

      long bytesRemaining = length;
      while (bytesRemaining >= maxPerSplit) {
        splits.add(new FileSplit(file, length-bytesRemaining, bytesPerSplit));
        bytesRemaining -= bytesPerSplit;
      }
      
      if (bytesRemaining != 0) {
        splits.add(new FileSplit(file, length-bytesRemaining, bytesRemaining));
      }
    }
    return (FileSplit[])splits.toArray(new FileSplit[splits.size()]);
  }

}


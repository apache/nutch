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
import org.apache.nutch.io.MapFile;
import org.apache.nutch.io.Writable;
import org.apache.nutch.io.WritableComparable;
import org.apache.nutch.io.LongWritable;
import org.apache.nutch.io.UTF8;

/** An {@link InputFormat} for {@link SequenceFile}s. */
public class SequenceFileInputFormat extends InputFormatBase {

  public SequenceFileInputFormat() {
    setMinSplitSize(SequenceFile.SYNC_INTERVAL);
  }

  protected File[] listFiles(NutchFileSystem fs, JobConf job)
    throws IOException {

    File[] files = super.listFiles(fs, job);
    for (int i = 0; i < files.length; i++) {
      File file = files[i];
      if (file.isDirectory()) {                   // it's a MapFile
        files[i] = new File(file, MapFile.DATA_FILE_NAME); // use the data file
      }
    }
    return files;
  }

  public RecordReader getRecordReader(NutchFileSystem fs, FileSplit split,
                                      JobConf job) throws IOException {

    return new SequenceFileRecordReader(fs, split);
  }

}


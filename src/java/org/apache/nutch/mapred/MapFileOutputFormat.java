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

import org.apache.nutch.io.MapFile;
import org.apache.nutch.io.WritableComparable;
import org.apache.nutch.io.Writable;

public class MapFileOutputFormat implements OutputFormat {

  public RecordWriter getRecordWriter(NutchFileSystem fs, JobConf job,
                                      String name) throws IOException {

    File file = new File(job.getOutputDir(), name);

    final MapFile.Writer out =
      new MapFile.Writer(fs, file.toString(),
                         job.getOutputKeyClass(),
                         job.getOutputValueClass(),
                         job.getBoolean("mapred.output.compress", false));

    return new RecordWriter() {

        public void write(WritableComparable key, Writable value)
          throws IOException {

          out.append(key, value);
        }

        public void close() throws IOException { out.close(); }
      };
  }      
}


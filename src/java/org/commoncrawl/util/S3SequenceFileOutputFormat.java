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
package org.commoncrawl.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * {@link SequenceFileOutputFormat} using {@link NullOutputCommitter} when
 * writing to AWS S3. See {@link S3FileOutputFormat}.
 */
public class S3SequenceFileOutputFormat<K, V> extends SequenceFileOutputFormat<K, V> {

  private OutputCommitter committer;

  @Override
  public synchronized OutputCommitter getOutputCommitter(
      TaskAttemptContext context) throws java.io.IOException {
    if (committer == null) {
      Path output = getOutputPath(context);

      String scheme = output.getFileSystem(context.getConfiguration())
          .getScheme();
      if (scheme.startsWith("s3")) {
        committer = new NullOutputCommitter();
      } else {
        committer = super.getOutputCommitter(context);
      }
    }
    return committer;
  }

}
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

package org.apache.nutch.mapred.lib;

import java.io.IOException;
import java.util.Iterator;

import org.apache.nutch.mapred.Reducer;
import org.apache.nutch.mapred.OutputCollector;
import org.apache.nutch.mapred.JobConf;
import org.apache.nutch.mapred.Reporter;

import org.apache.nutch.io.WritableComparable;
import org.apache.nutch.io.LongWritable;

/** A {@link Reducer} that sums long values. */
public class LongSumReducer implements Reducer {

  public void configure(JobConf job) {}

  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter)
    throws IOException {

    // sum all values for this key
    long sum = 0;
    while (values.hasNext()) {
      sum += ((LongWritable)values.next()).get();
    }

    // output sum
    output.collect(key, new LongWritable(sum));
  }
}

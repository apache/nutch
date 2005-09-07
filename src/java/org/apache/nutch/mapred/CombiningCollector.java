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

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.nutch.io.*;
import org.apache.nutch.util.*;

/** Implements partial value reduction during mapping.  This can minimize the
 * size of intermediate data.  Buffers a list of values for each unique key,
 * then invokes the combiner's reduce method to merge some values before
 * they're transferred to a reduce node. */
class CombiningCollector implements OutputCollector {
  private static final int LIMIT
    = NutchConf.get().getInt("mapred.combine.buffer.size", 100000);

  private int count = 0;
  private Map keyToValues;                        // the buffer

  private JobConf job;
  private OutputCollector out;
  private Reducer combiner;
  private Reporter reporter;

  public CombiningCollector(JobConf job, OutputCollector out,
                            Reporter reporter) {
    this.job = job;
    this.out = out;
    this.reporter = reporter;
    this.combiner = (Reducer)job.newInstance(job.getCombinerClass());
    this.keyToValues = new TreeMap(job.getOutputKeyComparator());
  }

  public synchronized void collect(WritableComparable key, Writable value)
    throws IOException {

    // buffer new value in map
    ArrayList values = (ArrayList)keyToValues.get(key);
    if (values == null) {                         // no values yet for this key
      values = new ArrayList(1);                  // make a new list
      values.add(value);                          // add this value
      keyToValues.put(key, values);               // add to map
    } else {
      values.add(value);                          // other values: just add new
    }

    count++;

    if (count >= LIMIT) {                         // time to flush
      flush();
    }
  }

  public synchronized void flush() throws IOException {
    Iterator pairs = keyToValues.entrySet().iterator();
    while (pairs.hasNext()) {
      Map.Entry pair = (Map.Entry)pairs.next();
      combiner.reduce((WritableComparable)pair.getKey(),
                      ((ArrayList)pair.getValue()).iterator(),
                      out, reporter);
    }
    keyToValues.clear();
    count = 0;
  }

}

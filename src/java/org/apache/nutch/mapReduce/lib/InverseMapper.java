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

package org.apache.nutch.mapReduce.lib;

import java.io.IOException;

import org.apache.nutch.mapReduce.Mapper;
import org.apache.nutch.mapReduce.OutputCollector;
import org.apache.nutch.mapReduce.JobConf;

import org.apache.nutch.io.WritableComparable;
import org.apache.nutch.io.Writable;

/** A {@link Mapper} that swaps keys and values. */
public class InverseMapper implements Mapper {

  public void configure(JobConf job) {}

  /** The inverse function.  Input keys and values are swapped.*/
  public void map(WritableComparable key, Writable value,
                  OutputCollector output) throws IOException {
    output.collect((WritableComparable)value, key);
  }
}

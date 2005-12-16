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

import org.apache.nutch.mapred.Mapper;
import org.apache.nutch.mapred.OutputCollector;
import org.apache.nutch.mapred.JobConf;
import org.apache.nutch.mapred.Reporter;

import org.apache.nutch.io.Writable;
import org.apache.nutch.io.WritableComparable;

/** Implements the identity function, mapping inputs directly to outputs. */
public class IdentityMapper implements Mapper {

  public void configure(JobConf job) {}

  /** The identify function.  Input key/value pair is written directly to
   * output.*/
  public void map(WritableComparable key, Writable val,
                  OutputCollector output, Reporter reporter)
    throws IOException {
    output.collect(key, val);
  }

}

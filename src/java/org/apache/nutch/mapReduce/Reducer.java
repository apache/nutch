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

import java.util.Iterator;

import org.apache.nutch.io.Writable;
import org.apache.nutch.io.WritableComparable;

/** Reduces a set of intermediate values which share a key to a smaller set of
 * values.  Input values are the grouped output of a {@link Mapper}. */
public interface Reducer extends Configurable {
  /** Combines values for a given key.  Output values must be of the same type
   * as input values.  Input keys must not be altered.  Typically all values
   * are combined into zero or one value.  Output pairs are collected with
   * calls to {@link OutputCollector#collect(WritableComparable,Writable)}.
   *
   * @param key the key
   * @param values the values to combine
   * @param output to collect combined values
   */
  void reduce(WritableComparable key, Iterator values, OutputCollector output)
    throws IOException;
}

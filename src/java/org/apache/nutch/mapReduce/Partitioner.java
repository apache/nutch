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

import org.apache.nutch.io.Writable;
import org.apache.nutch.io.WritableComparable;

/** Partitions the key space.  A partition is created for each reduce task. */
public interface Partitioner extends Configurable {
  /** Returns the paritition number for a given key given the total number of
   * partitions.  Typically a hash function on a all or a subset of the key.
   *
   * @param key the key
   * @param numPartitions the number of partitions
   * @return the partition number
   */
  int getPartition(WritableComparable key, int numPartitions);
}

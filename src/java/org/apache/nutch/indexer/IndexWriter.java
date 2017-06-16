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
package org.apache.nutch.indexer;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.plugin.Pluggable;

import java.io.IOException;
import java.util.Map;

public interface IndexWriter extends Pluggable, Configurable {
  /**
   * The name of the extension point.
   */
  final static String X_POINT_ID = IndexWriter.class.getName();

  @Deprecated
  public void open(JobConf job, String name) throws IOException;

  /**
   * Initializes the internal variables from a given index writer configuration.
   *
   * @param parameters Params from the index writer configuration.
   * @throws IOException Some exception thrown by writer.
   */
  void open(Map<String, String> parameters) throws IOException;

  public void write(NutchDocument doc) throws IOException;

  public void delete(String key) throws IOException;

  public void update(NutchDocument doc) throws IOException;

  public void commit() throws IOException;

  public void close() throws IOException;

  /**
   * Returns a String describing the IndexWriter instance and the specific parameters it can take.
   *
   * @return The full description.
   */
  public String describe();
}

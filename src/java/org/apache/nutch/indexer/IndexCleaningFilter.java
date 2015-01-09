/**
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

// Hadoop imports
import org.apache.hadoop.conf.Configurable;
import org.apache.nutch.plugin.FieldPluggable;
import org.apache.nutch.storage.WebPage;

/**
 * Extension point for indexing. Permits one to add metadata to the indexed
 * fields. All plugins found which implement this extension point are run
 * sequentially on the parse.
 */
public interface IndexCleaningFilter extends FieldPluggable, Configurable {
  /** The name of the extension point. */
  final static String X_POINT_ID = IndexCleaningFilter.class.getName();

  /**
   * @param url
   *          page url
   * @param page
   * @return true == remove false == keep
   * @throws IndexingException
   */
  boolean remove(String url, WebPage page) throws IndexingException;
}

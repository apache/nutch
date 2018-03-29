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

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.nutch.storage.WebPage;

/**
 * Extension point for duplicate indexing filtering. Permits one to determine (based on URL)
 * which duplicate webpage should be kept as the original.
 */
public interface DuplicateFilter extends Configurable {
  /** The name of the extension point. */
  final static String X_POINT_ID = DuplicateFilter.class.getName();

  /**
   * Determines which page represented is the original
   * page based on the list of URLs with duplicate content.
   * 
   * @param original
   *          the url of the page that was determined to be the original in the last iteration.
   * @param duplicates
   *          duplicate page URLs
   * @param webPages
   *          newly fetched and as yet un-indexed pages with duplicate content
   * @return the URL of the original page in the set of duplicates.
   * @throws IndexingException
   */
  CharSequence filter(CharSequence original, List<CharSequence> duplicates, Iterable<WebPage> webPages);
}

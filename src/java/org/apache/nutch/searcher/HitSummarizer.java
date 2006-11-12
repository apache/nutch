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

package org.apache.nutch.searcher;

import java.io.IOException;

/** Service that builds a summary for a hit on a query. */
public interface HitSummarizer {
  
  /**
   * Returns a summary for the given hit details.
   *
   * @param details the details of the hit to be summarized
   * @param query  indicates what should be higlighted in the summary text
   */
  Summary getSummary(HitDetails details, Query query) throws IOException;

  /**
   * Returns summaries for a set of details.  Hook for parallel IPC calls.
   *
   * @param details the details of hits to be summarized
   * @param query  indicates what should be higlighted in the summary text
   */
  Summary[] getSummary(HitDetails[] details, Query query) throws IOException;
}

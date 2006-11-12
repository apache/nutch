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

package org.creativecommons.nutch;

import org.apache.nutch.searcher.RawFieldQueryFilter;
import org.apache.hadoop.conf.Configuration;

/**
 * Handles "cc:" query clauses, causing them to search the "cc" field indexed by
 * CCIndexingFilter.
 */
public class CCQueryFilter extends RawFieldQueryFilter {
  private Configuration conf;

  public CCQueryFilter() {
    super(CCIndexingFilter.FIELD);
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    setBoost(conf.getFloat("query.cc.boost", 0.0f));
  }

  public Configuration getConf() {
    return this.conf;
  }
}

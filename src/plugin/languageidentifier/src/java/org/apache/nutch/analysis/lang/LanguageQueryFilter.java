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

package org.apache.nutch.analysis.lang;

import org.apache.nutch.searcher.RawFieldQueryFilter;
import org.apache.hadoop.conf.Configuration;

/** Handles "lang:" query clauses, causing them to search the "lang" field
 * indexed by LanguageIdentifier. */
public class LanguageQueryFilter extends RawFieldQueryFilter {
  private Configuration conf;

  public LanguageQueryFilter() {
    super("lang");
  }
  
  public void setConf(Configuration conf) {
    this.conf = conf;
    setBoost(conf.getFloat("query.lang.boost", 0.0f));
  }

  public Configuration getConf() {
    return this.conf;
  }
}

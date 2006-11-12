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
package org.apache.nutch.microformats.reltag;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;

// Nutch imports
import org.apache.nutch.searcher.RawFieldQueryFilter;


/**
 * Handles <code>"tag:"<code> query clauses.
 * 
 * @see <a href="http://www.microformats.org/wiki/rel-tag">
 *      http://www.microformats.org/wiki/rel-tag</a>
 * @author J&eacute;r&ocirc;me Charron
 */
public class RelTagQueryFilter extends RawFieldQueryFilter {
  
  private Configuration conf;

  public RelTagQueryFilter() {
    super("tag", true, 1.0f);
  }
  
  
  /* ----------------------------- *
   * <implementation:Configurable> *
   * ----------------------------- */

  public void setConf(Configuration conf) {
    this.conf = conf;
    setBoost(conf.getFloat("query.tag.boost", 1.0f));
  }

  public Configuration getConf() {
    return this.conf;
  }

  /* ------------------------------ *
   * </implementation:Configurable> *
   * ------------------------------ */

}

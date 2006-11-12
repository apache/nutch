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

// Hadoop imports
import org.apache.hadoop.conf.Configurable;

// Nutch imports
import org.apache.nutch.plugin.Pluggable;


/** 
 * Extension point for summarizer.
 *
 * @author J&eacute;r&ocirc;me Charron
 */
public interface Summarizer extends Configurable, Pluggable {

  /** The name of the extension point. */
  public final static String X_POINT_ID = Summarizer.class.getName();
  
  /**
   * Get a summary for a specified text.
   * @param text is the text to summarize.
   * @param query is the query for which the text is a hit.
   */
  public Summary getSummary(String text, Query query);

}

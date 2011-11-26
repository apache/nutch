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
package org.apache.nutch.segment;

import java.util.Collection;

import org.apache.hadoop.io.WritableComparable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;

/**
 * Interface used to filter segments during segment merge. It allows filtering
 * on more sophisticated criteria than just URLs. In particular it allows
 * filtering based on metadata collected while parsing page.
 * 
 */
public interface SegmentMergeFilter {
  /** The name of the extension point. */
  public final static String X_POINT_ID = SegmentMergeFilter.class.getName();

  /**
   * The filtering method which gets all information being merged for a given
   * key (URL).
   * 
   * @return <tt>true</tt> values for this <tt>key</tt> (URL) should be merged
   *         into the new segment.
   */
  public boolean filter(WritableComparable key, CrawlDatum generateData,
      CrawlDatum fetchData, CrawlDatum sigData, Content content,
      ParseData parseData, ParseText parseText, Collection<CrawlDatum> linked);
}

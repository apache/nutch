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
package org.apache.nutch.hostdb;

import org.apache.nutch.crawl.CrawlDatum;

/**
 * These are instantiated once for each host.
 */
public interface CrawlDatumProcessor {

  /**
   * Process a single crawl datum instance to aggregate custom counts.
   *
   * @param crawlDatum
   *          CrawlDatum instance to count information from
   */
  public void count(CrawlDatum crawlDatum);

  /**
   * Process the final host datum instance and store the aggregated custom
   * counts in the HostDatum.
   *
   * @param hostDatum
   *          HostDatum instance to hold the aggregated custom counts
   */
  public void finalize(HostDatum hostDatum);

}

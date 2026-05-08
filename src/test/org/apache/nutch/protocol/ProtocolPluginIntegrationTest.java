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
package org.apache.nutch.protocol;

import org.apache.nutch.crawl.CrawlDatum;

/**
 * Contract for Protocol plugin integration tests. Implementations run against
 * real server backends (via Testcontainers or embedded servers).
 */
public interface ProtocolPluginIntegrationTest {

  /** Set up the protocol plugin and its backing server before tests. */
  void setUpProtocol() throws Exception;

  /** Shut down the protocol plugin after tests. */
  void tearDownProtocol() throws Exception;

  /** The Protocol under test. */
  Protocol getProtocol();

  /**
   * A URL that the backing server will serve with a 200/success response.
   * Must point into the container or embedded server started by this test.
   */
  String getTestUrl();

  /**
   * Optional extra verification after a successful fetch.
   * Default is a no-op; override to inspect content, headers, etc.
   */
  default void verifyFetchedContent(ProtocolOutput output, CrawlDatum datum)
      throws Exception {
    // no-op
  }
}

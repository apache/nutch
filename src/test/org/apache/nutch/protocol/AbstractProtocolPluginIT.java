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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Abstract base for Protocol plugin integration tests using Testcontainers.
 * Provides common test logic for fetching URLs and verifying status codes.
 *
 * <p>Subclasses declare a static {@code @Container} field for the server
 * container, implement {@link ProtocolPluginIntegrationTest}, and may add
 * protocol-specific tests (e.g., redirect handling, authentication).
 */
@Testcontainers(disabledWithoutDocker = true)
public abstract class AbstractProtocolPluginIT implements ProtocolPluginIntegrationTest {

  @BeforeEach
  void setUp() throws Exception {
    setUpProtocol();
  }

  @AfterEach
  void tearDown() throws Exception {
    tearDownProtocol();
  }

  /** Fetch the test URL and assert an HTTP 200 response. */
  @Test
  void testFetch200() throws Exception {
    CrawlDatum datum = new CrawlDatum();
    ProtocolOutput output = getProtocol()
        .getProtocolOutput(new Text(getTestUrl()), datum);
    assertNotNull(output, "ProtocolOutput must not be null");
    assertEquals(200, getHttpStatusCode(datum),
        "Expected HTTP 200 for " + getTestUrl());
    verifyFetchedContent(output, datum);
  }

  /** Fetch a non-existent path and assert an HTTP 404 response. */
  @Test
  void testFetch404() throws Exception {
    String url = get404Url();
    CrawlDatum datum = new CrawlDatum();
    getProtocol().getProtocolOutput(new Text(url), datum);
    assertEquals(404, getHttpStatusCode(datum),
        "Expected HTTP 404 for " + url);
  }

  /**
   * Returns a URL expected to produce a 404. Default appends a random path
   * segment to {@link #getTestUrl()}; override if the server needs a specific
   * path.
   */
  protected String get404Url() {
    String base = getTestUrl();
    if (base.endsWith("/")) {
      return base + "nonexistent-path-xyz";
    }
    return base + "/nonexistent-path-xyz";
  }

  /**
   * Reads the HTTP status code stored in the CrawlDatum metadata by Nutch
   * protocol plugins. Returns -1 if no status code was stored.
   */
  protected static int getHttpStatusCode(CrawlDatum datum) {
    if (datum.getMetaData().containsKey(Nutch.PROTOCOL_STATUS_CODE_KEY)) {
      return Integer.parseInt(
          datum.getMetaData().get(Nutch.PROTOCOL_STATUS_CODE_KEY).toString());
    }
    return -1;
  }
}

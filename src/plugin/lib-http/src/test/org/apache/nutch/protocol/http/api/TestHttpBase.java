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
package org.apache.nutch.protocol.http.api;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test {@link HttpBase}
 */
public class TestHttpBase {

  /**
   * Test non-proxy hosts
   */
  @Test
  public void testIsProxyException() {
    final HttpBase base = new HttpBase() {

      @Override
      protected Response getResponse(URL url, CrawlDatum datum,
          boolean followRedirects) throws ProtocolException, IOException {
        return null;
      }

    };
    base.proxyException = new HashMap<>();

    // test exact match
    base.proxyException.put("some.domain.com", "some.domain.com");
    assertFalse(base.isProxyException("other.domain.com"));
    assertTrue(base.isProxyException("some.domain.com"));

    // test '*' prefix
    base.proxyException.clear();
    base.proxyException.put("*.domain.com", "*.domain.com");
    assertTrue(base.isProxyException("some.domain.com"));
    assertFalse(base.isProxyException("somedomain.com"));

    // test '*' suffix
    base.proxyException.clear();
    base.proxyException.put("some.domain.*", "some.domain.*");
    assertTrue(base.isProxyException("some.domain.with.long.name.com"));
    assertFalse(base.isProxyException("my.domain.com"));

  }

}

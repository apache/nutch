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
package org.commoncrawl.util;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestCanonicalLinkDetector {

  @ParameterizedTest
  @ValueSource(strings = {
      "<link href=\"https://www.example.org/canonical/\" rel=\"canonical\"/>", //
      "<link href=\"https://www.example.org/canonical/\" rel='canonical'/>", //
      "<link rel=\"canonical\" href=\"https://www.example.org/canonical/\" />", //
      "<link rel='canonical' href=\"https://www.example.org/canonical/\" />", //
      "<link   rel=canonical   href=\"https://www.example.org/canonical/\" />", //
      "<link rel=canonical\r\n       href=\"https://www.example.org/canonical/\" />", //
      "<link href=\"https://www.example.org/canonical/\"\n       rel=canonical />", //
      "<link rel=canonical href=https://www.example.org/canonical/ ><!-- HTML5 -->", //
      "<link rel=canonical href=\"/canonical/\" /><!-- relative URL -->", //
  })
  void testDetectHTML(String htmlSnippet) {
    String canonicalLink = "https://www.example.org/canonical/";
    List<String> canonicalLinks = CanonicalLinkDetector
        .detectCanonicalLinksHTML(htmlSnippet.getBytes(StandardCharsets.UTF_8),
            1024, 1);
    assertFalse(canonicalLinks.isEmpty());
    URI baseUri = URI.create("https://www.example.org/");
    assertEquals(canonicalLink,
        baseUri.resolve(canonicalLinks.get(0)).toASCIIString());
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "<https://www.example.org/canonical/>; rel=\"canonical\"", //
      "<https://www.example.org/canonical/>; rel=\"canonical\",<https://www.example.org/node/6568/>; rel=\"shortlink\"", //
      "<https://www.example.org/canonical/>; rel='canonical'", //
      "<https://www.example.org/canonical/>; rel=\"canonical\",<https://www.example.org/node/8098>; rel=\"shortlink\",<https://www.example.org/db.ico>; rel=\"shortcut icon\"", //
  })
  void testDetectHTTP(String httpHeader) {
    String canonicalLink = "https://www.example.org/canonical/";
    String[] linkHeaderValues = List.of(httpHeader).toArray(new String[0]);
    List<String> canonicalLinks = CanonicalLinkDetector
        .detectCanonicalLinksHttpHeader(linkHeaderValues, 1);
    assertFalse(canonicalLinks.isEmpty());
    assertEquals(canonicalLink, canonicalLinks.get(0));
  }
}

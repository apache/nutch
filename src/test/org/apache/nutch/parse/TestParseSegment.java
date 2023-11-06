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
package org.apache.nutch.parse;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import java.nio.charset.StandardCharsets;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.junit.Test;

public class TestParseSegment {

  @Test
  public void testMetadataFlag() throws Exception {
    Content content = new Content();
    Metadata metadata = new Metadata();
    metadata.set(Response.TRUNCATED_CONTENT, "true");
    content.setMetadata(metadata);
    content.setContent("the quick brown fox".getBytes(StandardCharsets.UTF_8));
    assertTrue(ParseSegment.isTruncated(content));
  }

  @Test
  public void testLength() throws Exception {
    byte[] bytes = "the quick brown fox".getBytes(StandardCharsets.UTF_8);
    Content content = new Content();
    Metadata metadata = new Metadata();
    metadata.set(Response.CONTENT_LENGTH, Integer.toString(bytes.length));
    content.setMetadata(metadata);
    content.setContent(bytes);
    assertFalse(ParseSegment.isTruncated(content));

    metadata.set(Response.CONTENT_LENGTH, Integer.toString(bytes.length * 2));
    assertTrue(ParseSegment.isTruncated(content));
  }
}

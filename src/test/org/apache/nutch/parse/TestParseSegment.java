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

import java.nio.charset.StandardCharsets;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestParseSegment {
  private static byte[] BYTES = "the quick brown fox".getBytes(StandardCharsets.UTF_8);

  @Test
  public void testMetadataFlag() throws Exception {

    Content content = new Content();
    Metadata metadata = new Metadata();
    metadata.set(Response.TRUNCATED_CONTENT, "true");
    content.setMetadata(metadata);
    content.setContent(BYTES);
    assertTrue(ParseSegment.isTruncated(content));

    metadata.set(Response.TRUNCATED_CONTENT, "false");
    assertFalse(ParseSegment.isTruncated(content));

    //test that truncated_content does override length field
    metadata = new Metadata();
    metadata.set(Response.TRUNCATED_CONTENT, "false");
    metadata.set(Response.CONTENT_LENGTH, Integer.toString(BYTES.length + 10));
    assertFalse(ParseSegment.isTruncated(content));

    //test that truncated_content does override length field
    metadata = new Metadata();
    metadata.set(Response.TRUNCATED_CONTENT, "true");
    metadata.set(Response.CONTENT_LENGTH, Integer.toString(BYTES.length));
    assertFalse(ParseSegment.isTruncated(content));

  }

  @Test
  public void testLength() throws Exception {
    Content content = new Content();
    Metadata metadata = new Metadata();
    metadata.set(Response.CONTENT_LENGTH, Integer.toString(BYTES.length));
    content.setMetadata(metadata);
    content.setContent(BYTES);
    assertFalse(ParseSegment.isTruncated(content));

    metadata.set(Response.CONTENT_LENGTH, Integer.toString(BYTES.length * 2));
    assertTrue(ParseSegment.isTruncated(content));
  }

  @Test
  public void testNoLengthField() {
    //test return false if there is no "Length" header field
    Content content = new Content();
    Metadata metadata = new Metadata();
    content.setMetadata(metadata);
    content.setContent(BYTES);
    assertFalse(ParseSegment.isTruncated(content));
  }
}

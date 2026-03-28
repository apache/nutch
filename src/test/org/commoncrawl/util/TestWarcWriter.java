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

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.protocol.Content;
import org.commoncrawl.util.test.SegmenterRecordReader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URI;
import java.util.Date;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestWarcWriter {

  @Test
  public void testWriteRevisitRecordContentType() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    WarcWriter writer = new WarcWriter(bos);

    File segmentDir = new File(System.getProperty("test.build.data", "."), "test-segments/20260224170658-revisit");
    assertNotNull(segmentDir, "Missing segment resource");
    String segmentPath = segmentDir.getAbsolutePath();
    String url = "https://de.wikipedia.org/wiki/Wikipedia:WikiCon_2025";

    Content content = SegmenterRecordReader.retrieveContent(segmentPath, url);
    URI targetUri = new URI(content.getUrl());

    Metadata metadata = content.getMetadata();
    String ip = content.getMetadata().get("_ip_");
    int httpStatusCode = 304;

    Date date = HttpDateFormat.toDate(metadata.get("date"));
    URI warcinfoId = writer.getRecordId();
    URI relatedId = writer.getRecordId();
    String warcProfile = WarcWriter.PROFILE_REVISIT_IDENTICAL_DIGEST;
    Date refersToDate = new Date(System.currentTimeMillis() - 3600000);
    String payloadDigest = "sha1:abc123";
    String blockDigest = "sha1:def456";

    writer.writeWarcRevisitRecord(targetUri, ip, httpStatusCode, date,
        warcinfoId, relatedId, warcProfile, refersToDate, payloadDigest,
        blockDigest, null, null, content.getContent(), content);

    byte[] compressed = bos.toByteArray();
    ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
    GZIPInputStream gis = new GZIPInputStream(bis);
    ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
    gis.transferTo(decompressed);

    String warcOutput = decompressed.toString();

    assertTrue(warcOutput.contains("WARC-Type: revisit"),
        "WARC record should have WARC-Type: revisit");
    assertTrue(warcOutput.contains("Content-Type: application/http; msgtype=response"),
        "WARC revisit record should have Content-Type: application/http; msgtype=response");
    assertTrue(warcOutput.contains("WARC-Refers-To-Target-URI: https://de.wikipedia.org/wiki/Wikipedia:WikiCon_2025"),
        "WARC record should have WARC-Refers-To-Target-URI header");
    assertTrue(warcOutput.contains("WARC-Profile: " + warcProfile),
        "WARC record should have WARC-Profile header");
  }
}

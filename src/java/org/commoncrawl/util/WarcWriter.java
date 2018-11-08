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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import org.apache.nutch.protocol.Content;

public class WarcWriter {
  protected OutputStream out = null;
  protected OutputStream origOut = null;

  private static final String WARC_VERSION = "WARC/1.0";

  // Record types
  private static final String WARC_INFO = "warcinfo";
  private static final String WARC_RESPONSE = "response";
  private static final String WARC_REQUEST = "request";
  private static final String WARC_REVISIT = "revisit";
  private static final String WARC_CONVERSION = "conversion";
  private static final String WARC_METADATA = "metadata";

  // Defined fields
  private static final String WARC_TYPE = "WARC-Type";
  private static final String WARC_DATE = "WARC-Date";
  private static final String WARC_RECORD_ID = "WARC-Record-ID";
  private static final String CONTENT_LENGTH = "Content-Length";
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String WARC_IP_ADDRESS = "WARC-IP-Address";
  private static final String WARC_WARCINFO_ID = "WARC-Warcinfo-ID";
  private static final String WARC_TARGET_URI = "WARC-Target-URI";
  private static final String WARC_CONCURRENT_TO = "WARC-Concurrent-To";
  private static final String WARC_REFERS_TO = "WARC-Refers-To";
  private static final String WARC_REFERS_TO_TARGET_URI = "WARC-Refers-To-Target-URI";
  private static final String WARC_REFERS_TO_DATE = "WARC-Refers-To-Date";
  private static final String WARC_BLOCK_DIGEST = "WARC-Block-Digest";
  private static final String WARC_PAYLOAD_DIGEST = "WARC-Payload-Digest";
  private static final String WARC_TRUNCATED = "WARC-Truncated";
  private static final String WARC_IDENTIFIED_PAYLOAD_TYPE = "WARC-Identified-Payload-Type";
  private static final String WARC_PROFILE = "WARC-Profile";
  private static final String WARC_FILENAME = "WARC-Filename";

  public static final String PROFILE_REVISIT_IDENTICAL_DIGEST = "http://netpreserve.org/warc/1.1/revisit/identical-payload-digest";
  public static final String PROFILE_REVISIT_NOT_MODIFIED = "http://netpreserve.org/warc/1.1/revisit/server-not-modified";

  private static final String CRLF = "\r\n";
  private static final String COLONSP = ": ";

  private SimpleDateFormat isoDate;

  public static class CompressedOutputStream extends GZIPOutputStream {
    public CompressedOutputStream(OutputStream out) throws IOException {
      super(out);
    }

    public void end() {
      def.end();
    }
  }

  public WarcWriter(final OutputStream out) {
    this.origOut = this.out = out;
    isoDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    isoDate.setTimeZone(TimeZone.getTimeZone("GMT"));
  }

  /**
   *
   * @return record id for the warcinfo record
   * @throws IOException
   */
  public URI writeWarcinfoRecord(String filename, String hostname,
      String publisher, String operator, String software, String isPartOf,
      String description, Date date)
      throws IOException {
    Map<String, String> extra = new LinkedHashMap<String, String>();
    extra.put(WARC_FILENAME, filename);

    StringBuilder sb = new StringBuilder();
    Map<String, String> settings = new LinkedHashMap<String, String>();

    if (isPartOf != null) {
      settings.put("isPartOf", isPartOf);
    }

    if (publisher != null) {
      settings.put("publisher", publisher);
    }

    if (description != null) {
      settings.put("description", description);
    }

    if (operator != null) {
      settings.put("operator", operator);
    }

    if (hostname != null) {
      settings.put("hostname", hostname);
    }

    if (software != null) {
      settings.put("software", software);
    }

    String robotsTxtParser = String.format(Locale.ROOT,
        "checked via crawler-commons %s (https://github.com/crawler-commons/crawler-commons)",
        crawlercommons.CrawlerCommons.getVersion());
    settings.put("robots", robotsTxtParser);

    settings.put("format", "WARC File Format 1.1");
    settings.put("conformsTo",
        "http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/");

    writeWarcKeyValue(sb, settings);

    byte[] ba = sb.toString().getBytes(StandardCharsets.UTF_8);
    URI recordId = getRecordId();

    writeRecord(WARC_INFO, date, "application/warc-fields", recordId, extra,
        new ByteArrayInputStream(ba), ba.length);
    return recordId;
  }

  public URI writeWarcRequestRecord(final URI targetUri, final String ip,
      final Date date, final URI warcinfoId, final byte[] block)
      throws IOException {
    Map<String, String> extra = new LinkedHashMap<String, String>();
    extra.put(WARC_WARCINFO_ID, "<" + warcinfoId.toString() + ">");
    extra.put(WARC_IP_ADDRESS, ip);
    extra.put(WARC_TARGET_URI, targetUri.toString());

    URI recordId = getRecordId();
    writeRecord(WARC_REQUEST, date, "application/http; msgtype=request",
        recordId, extra, block);
    return recordId;
  }

  public URI writeWarcResponseRecord(final URI targetUri, final String ip,
      final Date date, final URI warcinfoId, final URI relatedId,
      final String payloadDigest, final String blockDigest,
      final String truncated, final byte[] block, Content content)
      throws IOException {
    Map<String, String> extra = new LinkedHashMap<String, String>();
    extra.put(WARC_WARCINFO_ID, "<" + warcinfoId.toString() + ">");
    if (relatedId != null) {
      extra.put(WARC_CONCURRENT_TO, "<" + relatedId.toString() + ">");
    }
    extra.put(WARC_IP_ADDRESS, ip);
    extra.put(WARC_TARGET_URI, targetUri.toString());

    if (payloadDigest != null) {
      extra.put(WARC_PAYLOAD_DIGEST, payloadDigest);
    }

    if (blockDigest != null) {
      extra.put(WARC_BLOCK_DIGEST, blockDigest);
    }

    if (truncated != null) {
      extra.put(WARC_TRUNCATED, truncated);
    }

    extra.put(WARC_IDENTIFIED_PAYLOAD_TYPE, content.getContentType());

    URI recordId = getRecordId();
    writeRecord(WARC_RESPONSE, date, "application/http; msgtype=response",
        recordId, extra, block);
    return recordId;
  }

  public URI writeWarcRevisitRecord(final URI targetUri, final String ip,
      final Date date, final URI warcinfoId, final URI relatedId,
      final String warcProfile, final Date refersToDate,
      final String payloadDigest, final String blockDigest, byte[] block,
      Content content) throws IOException {
    Map<String, String> extra = new LinkedHashMap<String, String>();
    extra.put(WARC_WARCINFO_ID, "<" + warcinfoId.toString() + ">");
    extra.put(WARC_REFERS_TO, "<" + relatedId.toString() + ">");
    extra.put(WARC_IP_ADDRESS, ip);
    extra.put(WARC_TARGET_URI, targetUri.toString());
    // WARC-Refers-To-Target-URI only useful for revisit by digest
    extra.put(WARC_REFERS_TO_TARGET_URI, targetUri.toString());
    if (refersToDate != null) {
      extra.put(WARC_REFERS_TO_DATE, isoDate.format(refersToDate));
    }
    extra.put(WARC_PROFILE, warcProfile);

    if (payloadDigest != null) {
      extra.put(WARC_PAYLOAD_DIGEST, payloadDigest);
    }
    if (blockDigest != null) {
      extra.put(WARC_BLOCK_DIGEST, blockDigest);
    }

    URI recordId = getRecordId();
    writeRecord(WARC_REVISIT, date, "message/http", recordId, extra, block);
    return recordId;
  }

  public URI writeWarcMetadataRecord(final URI targetUri, final Date date,
      final URI warcinfoId, final URI relatedId, final String blockDigest,
      final byte[] block) throws IOException {
    Map<String, String> extra = new LinkedHashMap<String, String>();
    extra.put(WARC_WARCINFO_ID, "<" + warcinfoId.toString() + ">");
    extra.put(WARC_CONCURRENT_TO, "<" + relatedId.toString() + ">");
    extra.put(WARC_TARGET_URI, targetUri.toString());

    if (blockDigest != null) {
      extra.put(WARC_BLOCK_DIGEST, blockDigest);
    }

    URI recordId = getRecordId();
    writeRecord(WARC_METADATA, date, "application/warc-fields", recordId, extra,
        block);
    return recordId;
  }

  public URI writeWarcConversionRecord(final URI targetUri, final Date date,
      final URI warcinfoId, final URI relatedId, final String blockDigest,
      final String contentType, final byte[] block) throws IOException {
    Map<String, String> extra = new LinkedHashMap<String, String>();
    extra.put(WARC_WARCINFO_ID, "<" + warcinfoId.toString() + ">");
    extra.put(WARC_REFERS_TO, "<" + relatedId.toString() + ">");
    extra.put(WARC_TARGET_URI, targetUri.toString());

    if (blockDigest != null) {
      extra.put(WARC_BLOCK_DIGEST, blockDigest);
    }

    URI recordId = getRecordId();
    writeRecord(WARC_CONVERSION, date, contentType, recordId, extra, block);
    return recordId;
  }

  protected void writeRecord(final String type, final Date date,
      final String contentType, final URI recordId, Map<String, String> extra,
      final InputStream content, final long contentLength) throws IOException {
    StringBuilder sb = new StringBuilder(4096);

    sb.append(WARC_VERSION).append(CRLF);

    Map<String, String> header = new LinkedHashMap<String, String>();
    header.put(WARC_TYPE, type);
    header.put(WARC_DATE, isoDate.format(date));
    header.put(WARC_RECORD_ID, "<" + recordId.toString() + ">");
    header.put(CONTENT_LENGTH, Long.toString(contentLength));
    header.put(CONTENT_TYPE, contentType);

    writeWarcKeyValue(sb, header);
    writeWarcKeyValue(sb, extra);

    sb.append(CRLF);

    startRecord();
    out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
    if (contentLength != 0 && content != null) {
      copyStream(content, out, contentLength);
    }

    out.write(CRLF.getBytes());
    out.write(CRLF.getBytes());
    endRecord();
  }

  protected void writeRecord(final String type, final Date date,
      final String contentType, final URI recordId, Map<String, String> extra,
      final byte[] block) throws IOException {
    StringBuilder sb = new StringBuilder(4096);

    sb.append(WARC_VERSION).append(CRLF);

    Map<String, String> header = new LinkedHashMap<String, String>();
    header.put(WARC_TYPE, type);
    header.put(WARC_DATE, isoDate.format(date));
    header.put(WARC_RECORD_ID, "<" + recordId.toString() + ">");
    header.put(CONTENT_LENGTH, Long.toString(block.length));
    header.put(CONTENT_TYPE, contentType);

    writeWarcKeyValue(sb, header);
    writeWarcKeyValue(sb, extra);

    sb.append(CRLF);

    startRecord();
    out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
    if (block != null && block.length != 0) {
      out.write(block);
    }

    out.write(CRLF.getBytes());
    out.write(CRLF.getBytes());
    endRecord();
  }

  protected void startRecord() throws IOException {
    this.out = new CompressedOutputStream(this.origOut);
  }

  protected void endRecord() throws IOException {
    CompressedOutputStream compressedOut = (CompressedOutputStream) this.out;
    compressedOut.finish();
    compressedOut.flush();
    compressedOut.end();

    this.out = this.origOut;
  }

  protected long copyStream(InputStream input, OutputStream output,
      long maxBytes) throws IOException {
    byte[] buffer = new byte[4096];
    long count = 0L;
    int n = 0;
    while (-1 != (n = input.read(buffer))) {
      if (maxBytes > 0 && maxBytes < n) {
        n = (int) maxBytes;
      }

      output.write(buffer, 0, n);
      count += n;
      maxBytes -= n;

      if (maxBytes == 0) {
        return count;
      }
    }
    return count;
  }

  protected void writeWarcKeyValue(StringBuilder sb,
      Map<String, String> headers) {
    if (headers != null) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        sb.append(entry.getKey()).append(COLONSP).append(entry.getValue())
            .append(CRLF);
      }
    }
  }

  private String getUUID() {
    return UUID.randomUUID().toString();
  }

  public URI getRecordId() {
    try {
      return new URI("urn:uuid:" + getUUID());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}

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
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

public class WarcWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

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
  /** WARC-Protocol, see https://github.com/iipc/warc-specifications/issues/42 */
  private static final String WARC_PROTOCOL = "WARC-Protocol";
  /** WARC-Cipher-Suite, see https://github.com/iipc/warc-specifications/issues/94 */
  private static final String WARC_CIPHER_SUITE = "WARC-Cipher-Suite";

  public static final String PROFILE_REVISIT_IDENTICAL_DIGEST = "http://netpreserve.org/warc/1.1/revisit/identical-payload-digest";
  public static final String PROFILE_REVISIT_NOT_MODIFIED = "http://netpreserve.org/warc/1.1/revisit/server-not-modified";

  private static final String CRLF = "\r\n";
  private static final String COLONSP = ": ";

  /* Metadata names to pass from WARC to CDX */
  protected static final String HTTP_STATUS_CODE = "HTTP-Status-Code";
  protected static final String DETECTED_CHARSET = "Detected-Charset";
  protected static final String DETECTED_LANGUAGE = "Detected-Language";

  public static final String CONTENT_TYPE_RESPONSE = "application/http; msgtype=response";
  public static final String CONTENT_TYPE_METADATA = "application/warc-fields";

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
    isoDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT);
    isoDate.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  /**
   * Class to hold HTTP and SSL/TLS protocol versions to fill the
   * <code>WARC-Protocol</code> field. Protocol names require normalization, see
   * https://github.com/iipc/warc-specifications/issues/42
   */
  public static class WarcProtocol {
    public static Set<String> protocols = Set.of("dns", "ftp", "gemini",
        "gopher", "http/0.9", "http/1.0", "http/1.1", "h2", "h2c", "spdy/1",
        "spdy/2", "spdy/3", "ssl/2", "ssl/3", "tls/1.0", "tls/1.1", "tls/1.2",
        "tls/1.3");
    public static Pattern vPattern = Pattern.compile("^(?:ssl|tls)v[0-9]",
        Pattern.CASE_INSENSITIVE);
    private String name;

    public WarcProtocol(final String protocol) {
      name = protocol.toLowerCase(Locale.ROOT);
      if (vPattern.matcher(name).find()) {
        name = name.substring(0, 3) + '/' + name.substring(4);
      }
      if (!protocols.contains(name)) {
        LOG.warn("Unknown protocol name or version: {}", name);
      }
    }

    @Override
    public String toString() {
      return name;
    }
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
    Multimap<String, String> extra = LinkedListMultimap.create();
    extra.put(WARC_FILENAME, filename);

    StringBuilder sb = new StringBuilder();
    Multimap<String, String> settings = LinkedListMultimap.create();

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
        "https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/");

    writeWarcKeyValue(sb, settings);

    byte[] ba = sb.toString().getBytes(StandardCharsets.UTF_8);
    URI recordId = getRecordId();

    writeRecord(WARC_INFO, date, CONTENT_TYPE_METADATA, recordId, extra,
        new ByteArrayInputStream(ba), ba.length);
    return recordId;
  }

  public URI writeWarcRequestRecord(final URI targetUri, final String ip,
      final Date date, final URI warcinfoId, String[] protocolVersions,
      String[] cipherSuites, final byte[] block) throws IOException {
    Multimap<String, String> extra = LinkedListMultimap.create();
    extra.put(WARC_WARCINFO_ID, "<" + warcinfoId.toString() + ">");
    extra.put(WARC_IP_ADDRESS, ip);
    extra.put(WARC_TARGET_URI, targetUri.toASCIIString());
    if (protocolVersions != null) {
      for (String pVersion : protocolVersions) {
        extra.put(WARC_PROTOCOL, new WarcProtocol(pVersion).toString());
      }
    }
    if (cipherSuites != null) {
      for (String cipher : cipherSuites) {
        extra.put(WARC_CIPHER_SUITE, cipher);
      }
    }

    URI recordId = getRecordId();
    writeRecord(WARC_REQUEST, date, "application/http; msgtype=request",
        recordId, extra, block);
    return recordId;
  }

  public URI writeWarcResponseRecord(final URI targetUri, final String ip,
      final int httpStatusCode, final Date date, final URI warcinfoId,
      final URI relatedId, final String payloadDigest, final String blockDigest,
      final String truncated, String[] protocolVersions, String[] cipherSuites,
      final byte[] block, Content content) throws IOException {
    Multimap<String, String> extra = LinkedListMultimap.create();
    extra.put(WARC_WARCINFO_ID, "<" + warcinfoId.toString() + ">");
    if (relatedId != null) {
      extra.put(WARC_CONCURRENT_TO, "<" + relatedId.toString() + ">");
    }
    extra.put(WARC_IP_ADDRESS, ip);
    extra.put(WARC_TARGET_URI, targetUri.toASCIIString());
    if (protocolVersions != null) {
      for (String pVersion : protocolVersions) {
        extra.put(WARC_PROTOCOL, new WarcProtocol(pVersion).toString());
      }
    }
    if (cipherSuites != null) {
      for (String cipher : cipherSuites) {
        extra.put(WARC_CIPHER_SUITE, cipher);
      }
    }

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
    writeRecord(WARC_RESPONSE, date, CONTENT_TYPE_RESPONSE, recordId, extra, block);
    return recordId;
  }

  public URI writeWarcRevisitRecord(final URI targetUri, final String ip,
      final int httpStatusCode, final Date date, final URI warcinfoId,
      final URI relatedId, final String warcProfile, final Date refersToDate,
      final String payloadDigest, final String blockDigest,
      String[] protocolVersions, String[] cipherSuites, byte[] block,
      Content content) throws IOException {
    Multimap<String, String> extra = LinkedListMultimap.create();
    extra.put(WARC_WARCINFO_ID, "<" + warcinfoId.toString() + ">");
    extra.put(WARC_REFERS_TO, "<" + relatedId.toString() + ">");
    extra.put(WARC_IP_ADDRESS, ip);
    extra.put(WARC_TARGET_URI, targetUri.toASCIIString());
    if (protocolVersions != null) {
      for (String pVersion : protocolVersions) {
        extra.put(WARC_PROTOCOL, new WarcProtocol(pVersion).toString());
      }
    }
    if (cipherSuites != null) {
      for (String cipher : cipherSuites) {
        extra.put(WARC_CIPHER_SUITE, cipher);
      }
    }
    // WARC-Refers-To-Target-URI only useful for revisit by digest
    extra.put(WARC_REFERS_TO_TARGET_URI, targetUri.toASCIIString());
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
    writeRecord(WARC_REVISIT, date, CONTENT_TYPE_RESPONSE, recordId, extra, block);
    return recordId;
  }

  public URI writeWarcMetadataRecord(final URI targetUri, final Date date,
      final URI warcinfoId, final URI relatedId, final String blockDigest,
      final byte[] block) throws IOException {
    Multimap<String, String> extra = LinkedListMultimap.create();
    extra.put(WARC_WARCINFO_ID, "<" + warcinfoId.toString() + ">");
    extra.put(WARC_CONCURRENT_TO, "<" + relatedId.toString() + ">");
    extra.put(WARC_TARGET_URI, targetUri.toASCIIString());

    if (blockDigest != null) {
      extra.put(WARC_BLOCK_DIGEST, blockDigest);
    }

    URI recordId = getRecordId();
    writeRecord(WARC_METADATA, date, CONTENT_TYPE_METADATA, recordId, extra, block);
    return recordId;
  }

  public URI writeWarcConversionRecord(final URI targetUri, final Date date,
      final URI warcinfoId, final URI relatedId, final String blockDigest,
      final String contentType, final byte[] block) throws IOException {
    Multimap<String, String> extra = LinkedListMultimap.create();
    extra.put(WARC_WARCINFO_ID, "<" + warcinfoId.toString() + ">");
    extra.put(WARC_REFERS_TO, "<" + relatedId.toString() + ">");
    extra.put(WARC_TARGET_URI, targetUri.toASCIIString());

    if (blockDigest != null) {
      extra.put(WARC_BLOCK_DIGEST, blockDigest);
    }

    URI recordId = getRecordId();
    writeRecord(WARC_CONVERSION, date, contentType, recordId, extra, block);
    return recordId;
  }

  protected void writeRecord(final String type, final Date date,
      final String contentType, final URI recordId,
      Multimap<String, String> extra, final InputStream content,
      final long contentLength) throws IOException {
    StringBuilder sb = new StringBuilder(4096);

    sb.append(WARC_VERSION).append(CRLF);

    Multimap<String, String> header = LinkedListMultimap.create();
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
      final String contentType, final URI recordId,
      Multimap<String, String> extra, final byte[] block) throws IOException {
    StringBuilder sb = new StringBuilder(4096);

    sb.append(WARC_VERSION).append(CRLF);

    Multimap<String, String> header = LinkedListMultimap.create();
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
    out.write(block);

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

  protected static void writeWarcKeyValue(StringBuilder sb,
      Map<String, String> headers) {
    if (headers != null) {
      headers.forEach((k, v) -> writeWarcKeyValue(sb, k, v));
    }
  }

  protected static void writeWarcKeyValue(StringBuilder sb,
      Multimap<String, String> headers) {
    if (headers != null) {
      headers.forEach((k, v) -> writeWarcKeyValue(sb, k, v));
    }
  }

  protected static void writeWarcKeyValue(StringBuilder sb, String key,
      String value) {
    sb.append(key).append(COLONSP).append(value).append(CRLF);
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

  protected static String getMeta(Metadata metadata, String name) {
    String value = metadata.get(name);
    if (value == null) {
      // check for case variants
      for (String n : metadata.names()) {
        if (n.equalsIgnoreCase(name)) {
          value = metadata.get(n);
          break;
        }
      }
    }
    return value;
  }

}

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

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.io.output.CountingOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.archive.url.WaybackURLKeyMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class WarcCdxWriter extends WarcWriter {

  public static Logger LOG = LoggerFactory.getLogger(WarcCdxWriter.class);

  private static final Charset UTF_8 = StandardCharsets.UTF_8;

  protected CountingOutputStream countingOut;
  protected OutputStream cdxOut;
  protected String warcFilename;

  private SimpleDateFormat timestampFormat;
  private ObjectWriter jsonWriter;
  private WaybackURLKeyMaker surtKeyMaker = new WaybackURLKeyMaker(true);

  /**
   * JSON indentation same as by Python WayBack
   * (https://github.com/ikreymer/pywb)
   */
  @SuppressWarnings("serial")
  public static class JsonIndenter extends MinimalPrettyPrinter {

    // @Override
    @Override
    public void writeObjectFieldValueSeparator(JsonGenerator jg)
        throws IOException, JsonGenerationException {
      jg.writeRaw(": ");
    }

    // @Override
    @Override
    public void writeObjectEntrySeparator(JsonGenerator jg)
        throws IOException, JsonGenerationException {
      jg.writeRaw(", ");
    }
  }

  public WarcCdxWriter(OutputStream warcOut, OutputStream cdxOut,
      Path warcFilePath) {
    super(new CountingOutputStream(warcOut));
    countingOut = (CountingOutputStream) this.out;
    this.cdxOut = cdxOut;
    timestampFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    timestampFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    warcFilename = warcFilePath.toUri().getPath().replaceFirst("^/", "");
    ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.getFactory().configure(JsonGenerator.Feature.ESCAPE_NON_ASCII,
        true);
    jsonWriter = jsonMapper.writer(new JsonIndenter());
  }

  @Override
  public URI writeWarcRevisitRecord(final URI targetUri, final String ip,
      final int httpStatusCode, final Date date, final URI warcinfoId,
      final URI relatedId, final String warcProfile, final Date refersToDate,
      final String payloadDigest, final String blockDigest, byte[] block,
      Content content) throws IOException {
    long offset = countingOut.getByteCount();
    URI recordId = super.writeWarcRevisitRecord(targetUri, ip, httpStatusCode,
        date, warcinfoId, relatedId, warcProfile, refersToDate, payloadDigest,
        blockDigest, block, content);
    long length = (countingOut.getByteCount() - offset);
    writeCdxLine(targetUri, date, offset, length, payloadDigest, content, true,
        null, null);
    return recordId;
  }

  @Override
  public URI writeWarcResponseRecord(final URI targetUri, final String ip,
      final int httpStatusCode, final Date date, final URI warcinfoId,
      final URI relatedId, final String payloadDigest, final String blockDigest,
      final String truncated, final byte[] block, Content content)
      throws IOException {
    long offset = countingOut.getByteCount();
    URI recordId = super.writeWarcResponseRecord(targetUri, ip, httpStatusCode,
        date, warcinfoId, relatedId, payloadDigest, blockDigest, truncated,
        block, content);
    long length = (countingOut.getByteCount() - offset);
    String redirectLocation = null;
    if (isRedirect(httpStatusCode)) {
      redirectLocation = getMeta(content.getMetadata(), "Location");
    }
    writeCdxLine(targetUri, date, offset, length, payloadDigest, content, false,
        redirectLocation, truncated);
    return recordId;
  }

  public void writeCdxLine(final URI targetUri, final Date date, long offset,
      long length, String payloadDigest, Content content, boolean revisit,
      String redirectLocation, String truncated) throws IOException {
    String url = targetUri.toASCIIString();
    String surt = url;
    Metadata meta = content.getMetadata();
    try {
      surt = surtKeyMaker.makeKey(url);
    } catch (URISyntaxException e) {
      LOG.error("Failed to make SURT for {}: {}", url,
          StringUtils.stringifyException(e));
      return;
    }
    if (payloadDigest == null) {
      // no content, e.g., revisit record
    } else if (payloadDigest.startsWith("sha1:")) {
      payloadDigest = payloadDigest.substring(5);
    }
    cdxOut.write(surt.getBytes(UTF_8));
    cdxOut.write(' ');
    cdxOut.write(timestampFormat.format(date).getBytes(UTF_8));
    cdxOut.write(' ');
    Map<String, String> data = new LinkedHashMap<String, String>();
    data.put("url", url);
    if (revisit) {
      data.put("mime", "warc/revisit");
    } else {
      data.put("mime", cleanMimeType(getMeta(meta, Response.CONTENT_TYPE)));
      data.put("mime-detected", content.getContentType());
    }
    data.put("status", meta.get(WarcWriter.HTTP_STATUS_CODE));
    if (payloadDigest != null) {
      data.put("digest", payloadDigest);
    }
    data.put("length", String.format("%d", length));
    data.put("offset", String.format("%d", offset));
    data.put("filename", warcFilename);
    String val = meta.get(WarcWriter.DETECTED_CHARSET);
    if (val != null) {
      data.put("charset", val);
    }
    val = meta.get(WarcWriter.DETECTED_LANGUAGE);
    if (val != null) {
      data.put("languages", val);
    }
    if (truncated != null) {
      data.put("truncated", truncated);
    }
    if (redirectLocation != null) {
      data.put("redirect", redirectLocation);
    }
    cdxOut.write(jsonWriter.writeValueAsBytes(data));
    cdxOut.write('\n');
  }

  protected static String cleanMimeType(String mime) {
    if (mime == null)
      return "unk";
    final char[] delimiters = { ';', ' ' };
    for (char delim : delimiters) {
      int pos = mime.indexOf(delim);
      if (pos > -1)
        mime = mime.substring(0, pos);
    }
    if (mime.isEmpty())
      return "unk";
    return mime;
  }

  protected static boolean isRedirect(int httpStatusCode) {
    return httpStatusCode >= 300 && httpStatusCode < 400
        && httpStatusCode != 304;
  }
}

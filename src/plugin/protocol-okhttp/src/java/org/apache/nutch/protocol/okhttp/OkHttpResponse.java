/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.protocol.okhttp;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.Base64;
import java.util.Locale;

import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.Request;
import okhttp3.ResponseBody;
import okio.BufferedSource;

public class OkHttpResponse implements Response {

  protected static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private URL url;
  private byte[] content;
  private int code;
  private Metadata headers = new Metadata();

  /** Container to store whether and why content has been truncated */
  public static class TruncatedContent {

    private TruncatedContentReason value = TruncatedContentReason.NOT_TRUNCATED;

    public TruncatedContent() {
    }

    public void setReason(TruncatedContentReason val) {
      value = val;
   }

    public TruncatedContentReason getReason() {
       return value;
    }

    public boolean booleanValue() {
      return value != TruncatedContentReason.NOT_TRUNCATED;
    }
  }

  public OkHttpResponse(OkHttp okhttp, URL url, CrawlDatum datum)
      throws ProtocolException, IOException {

    this.url = url;

    Request.Builder rb = new Request.Builder().url(url);

    rb.header(USER_AGENT, okhttp.getUserAgent());
    okhttp.getCustomRequestHeaders().forEach((k) -> {
        rb.header(k[0], k[1]);
    });

    if (okhttp.isIfModifiedSinceEnabled() && datum.getModifiedTime() > 0) {
      rb.header(IF_MODIFIED_SINCE,
          HttpDateFormat.toString(datum.getModifiedTime()));
    }

    if (okhttp.isCookieEnabled()) {
      String cookie = null;
      
      if (datum.getMetaData().containsKey(HttpBase.COOKIE)) {
        cookie = ((Text)datum.getMetaData().get(HttpBase.COOKIE)).toString();
      }
      
      if (cookie == null) {
        cookie = okhttp.getCookie(url);
      }
      
      if (cookie != null) {
        rb.header("Cookie", cookie);
      }
    }

    Request request = rb.build();
    okhttp3.Call call = okhttp.getClient().newCall(request);

    // ensure that Response and underlying ResponseBody are closed
    try (okhttp3.Response response = call.execute()) {

      Metadata responsemetadata = new Metadata();
      okhttp3.Headers httpHeaders = response.headers();

      for (int i = 0, size = httpHeaders.size(); i < size; i++) {
        String key = httpHeaders.name(i);
        String value = httpHeaders.value(i);

        if (key.equals(REQUEST) || key.equals(RESPONSE_HEADERS)) {
          value = new String(Base64.getDecoder().decode(value));
        }

        responsemetadata.add(key, value);
      }
      LOG.debug("{} - {} {} {}", url, response.protocol(), response.code(),
          response.message());

      TruncatedContent truncated = new TruncatedContent();
      content = toByteArray(response.body(), truncated, okhttp.getMaxContent(),
          okhttp.getMaxDuration(), okhttp.isStorePartialAsTruncated());
      responsemetadata.add(FETCH_TIME,
          Long.toString(System.currentTimeMillis()));
      if (truncated.booleanValue()) {
        if (!call.isCanceled()) {
          call.cancel();
        }
        responsemetadata.set(TRUNCATED_CONTENT, "true");
        responsemetadata.set(TRUNCATED_CONTENT_REASON,
            truncated.getReason().toString().toLowerCase(Locale.ROOT));
        LOG.debug("HTTP content truncated to {} bytes (reason: {})",
            content.length, truncated.getReason());
      }

      code = response.code();
      headers = responsemetadata;
    }
  }

  private final byte[] toByteArray(final ResponseBody responseBody,
      TruncatedContent truncated, int maxContent, int maxDuration,
      boolean partialAsTruncated) throws IOException {

    if (responseBody == null) {
      return new byte[] {};
    }

    long endDueFor = -1;
    if (maxDuration != -1) {
      endDueFor = System.currentTimeMillis() + (maxDuration * 1000);
    }

    int maxContentBytes = Integer.MAX_VALUE;
    if (maxContent >= 0) {
      maxContentBytes = Math.min(maxContentBytes, maxContent);
    }

    BufferedSource source = responseBody.source();
    int bytesRequested = 0;
    int bufferGrowStepBytes = 8192;
    while (source.buffer().size() <= maxContentBytes) {
      bytesRequested += Math.min(bufferGrowStepBytes,
          /*
           * request one byte more than required to reliably detect truncated
           * content, but beware of integer overflows
           */
          (maxContentBytes == Integer.MAX_VALUE ? maxContentBytes
              : (1 + maxContentBytes)) - bytesRequested);
      boolean success = false;
      try {
        success = source.request(bytesRequested);
      } catch (IOException e) {
        if (partialAsTruncated && source.buffer().size() > 0) {
          // treat already fetched content as truncated
          truncated.setReason(TruncatedContentReason.DISCONNECT);
        } else {
          throw e;
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("total bytes requested = {}, buffered = {}", bytesRequested,
            source.buffer().size());
      }
      if (!success) {
        LOG.debug("source exhausted, no more data to read");
        break;
      }
      if (endDueFor != -1 && endDueFor <= System.currentTimeMillis()) {
        LOG.debug("max. fetch duration reached");
        truncated.setReason(TruncatedContentReason.TIME);
        break;
      }
      if (source.buffer().size() >= maxContentBytes) {
        LOG.debug("content limit reached");
      }
      // okhttp may fetch more content than requested, forward requested bytes
      bytesRequested = (int) source.buffer().size();
    }
    int bytesBuffered = (int) source.buffer().size();
    int bytesToCopy = bytesBuffered;
    if (maxContent >= 0 && bytesToCopy > maxContent) {
      // okhttp's internal buffer is larger than maxContent
      truncated.setReason(TruncatedContentReason.LENGTH);
      bytesToCopy = maxContentBytes;
    }
    byte[] arr = new byte[bytesToCopy];
    source.buffer().readFully(arr);
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "copied {} bytes out of {} buffered, remaining {} bytes in buffer",
          bytesToCopy, bytesBuffered, source.buffer().size());
    }
    return arr;
  }

  public URL getUrl() {
    return url;
  }

  public int getCode() {
    return code;
  }

  public String getHeader(String name) {
    return headers.get(name);
  }

  public Metadata getHeaders() {
    return headers;
  }

  public byte[] getContent() {
    return content;
  }

}

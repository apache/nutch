/**
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

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
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

    Request request = rb.build();
    okhttp3.Call call = okhttp.getClient().newCall(request);

    try (okhttp3.Response response = call.execute()) {

      Metadata responsemetadata = new Metadata();
      okhttp3.Headers httpHeaders = response.headers();

      for (int i = 0, size = httpHeaders.size(); i < size; i++) {
        String key = httpHeaders.name(i);
        String value = httpHeaders.value(i);

        if (key.equals(REQUEST)
            || key.equals(RESPONSE_HEADERS)) {
          value = new String(Base64.getDecoder().decode(value));
        }

        responsemetadata.add(key, value);
      }
      LOG.debug("{} - {} {} {}", url, response.protocol(), response.code(),
          response.message());

      MutableBoolean trimmed = new MutableBoolean();
      content = toByteArray(response.body(), trimmed, okhttp.getMaxContent(),
          okhttp.getTimeout());
      responsemetadata.add(FETCH_TIME,
          Long.toString(System.currentTimeMillis()));
      if (trimmed.booleanValue()) {
        if (!call.isCanceled()) {
          call.cancel();
        }
        responsemetadata.set(TRIMMED_CONTENT, "true");
        LOG.debug("HTTP content trimmed to {} bytes", content.length);
      }

      code = response.code();
      headers = responsemetadata;

    } catch (IOException e) {
      LOG.warn("Fetch of URL {} failed: {}", url, e);
    }

  }

  private final byte[] toByteArray(final ResponseBody responseBody,
      MutableBoolean trimmed, int maxContent, int timeout) throws IOException {

    if (responseBody == null) {
      return new byte[] {};
    }

    long endDueFor = -1;
    if (timeout != -1) {
      endDueFor = System.currentTimeMillis() + timeout;
    }

    int maxContentBytes = Integer.MAX_VALUE;
    if (maxContent != -1) {
      maxContentBytes = Math.min(maxContentBytes, maxContent);
    }

    BufferedSource source = responseBody.source();
    int contentBytesBuffered = 0;
    int contentBytesRequested = 0;
    int bufferGrowStepBytes = 8192;
    while (contentBytesBuffered < maxContentBytes) {
      contentBytesRequested += Math.min(bufferGrowStepBytes,
          (maxContentBytes - contentBytesBuffered));
      boolean success = source.request(contentBytesRequested);
      contentBytesBuffered = (int) source.buffer().size();
      if (LOG.isDebugEnabled()) {
        LOG.debug("total bytes requested = {}, buffered = {}",
            contentBytesRequested, contentBytesBuffered);
      }
      if (!success) {
        LOG.debug("source exhausted, no more data to read");
        break;
      }
      if (endDueFor != -1 && endDueFor <= System.currentTimeMillis()) {
        LOG.debug("timeout reached");
        trimmed.setValue(true);
        break;
      }
      if (contentBytesBuffered > maxContentBytes) {
        LOG.debug("content limit reached");
        trimmed.setValue(true);
      }
    }
    if (maxContent != -1 && contentBytesBuffered > maxContent) {
      // okhttp's internal buffer is larger than maxContent
      trimmed.setValue(true);
      contentBytesBuffered = maxContentBytes;
    }
    byte[] arr = new byte[contentBytesBuffered];
    source.buffer().read(arr);
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

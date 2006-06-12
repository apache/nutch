/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.protocol.httpclient;

// JDK imports
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// HTTP Client imports
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

// Nutch imports
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.nutch.util.LogUtil;


/**
 * An HTTP response.
 */
public class HttpResponse implements Response {

  public final static Log LOG = LogFactory.getLog(HttpResponse.class);

  private URL url;
  
  private String orig;

  private String base;

  private byte[] content;

  private HttpBase http;

  private int code;

  private Metadata headers = new Metadata();

  
  public HttpResponse(HttpBase http, URL url, CrawlDatum datum) throws IOException {
    this(http, url, datum, false);
  }

  
  HttpResponse(HttpBase http, URL url, CrawlDatum datum, boolean followRedirects) throws IOException {
    this.http = http;
    this.url = url;
    this.base = url.toString();
    this.orig = url.toString();
    GetMethod get = new GetMethod(this.orig);
    get.setFollowRedirects(followRedirects);
    get.setRequestHeader("User-Agent", http.getUserAgent());
    HttpMethodParams params = get.getParams();
    if (http.getUseHttp11()) {
      params.setVersion(HttpVersion.HTTP_1_1);
    } else {
      params.setVersion(HttpVersion.HTTP_1_0);
    }
    params.makeLenient();
    params.setContentCharset("UTF-8");
    params.setCookiePolicy(CookiePolicy.BROWSER_COMPATIBILITY);
    params.setBooleanParameter(HttpMethodParams.SINGLE_COOKIE_HEADER, true);
    // XXX (ab) not sure about this... the default is to retry 3 times; if
    // XXX the request body was sent the method is not retried, so there is
    // XXX little danger in retrying...
    // params.setParameter(HttpMethodParams.RETRY_HANDLER, null);
    try {
      code = Http.getClient().executeMethod(get);

      Header[] heads = get.getResponseHeaders();

      for (int i = 0; i < heads.length; i++) {
        headers.set(heads[i].getName(), heads[i].getValue());
      }
      
      // always read content. Sometimes content is useful to find a cause
      // for error.
      try {
        InputStream in = get.getResponseBodyAsStream();
        byte[] buffer = new byte[http.BUFFER_SIZE];
        int bufferFilled = 0;
        int totalRead = 0;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int tryAndRead = calculateTryToRead(totalRead);
        while ((bufferFilled = in.read(buffer, 0, buffer.length)) != -1 && tryAndRead > 0) {
          totalRead += bufferFilled;
          out.write(buffer, 0, bufferFilled);
          tryAndRead = calculateTryToRead(totalRead);
        }

        content = out.toByteArray();
        in.close();
      } catch (Exception e) {
        if (code == 200) throw new IOException(e.toString());
        // for codes other than 200 OK, we are fine with empty content
      }
      if (content != null) {
        // check if we have to uncompress it
        String contentEncoding = headers.get(Response.CONTENT_ENCODING);
        if ("gzip".equals(contentEncoding) || "x-gzip".equals(contentEncoding)) {
          content = http.processGzipEncoded(content, url);
        }
      }
    } catch (org.apache.commons.httpclient.ProtocolException pe) {
      pe.printStackTrace(LogUtil.getErrorStream(LOG));
      get.releaseConnection();
      throw new IOException(pe.toString());
    } finally {
      get.releaseConnection();
    }
  }

  
  /* ------------------------- *
   * <implementation:Response> *
   * ------------------------- */
  
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

  /* -------------------------- *
   * </implementation:Response> *
   * -------------------------- */

  

  private int calculateTryToRead(int totalRead) {
    int tryToRead = Http.BUFFER_SIZE;
    if (http.getMaxContent() <= 0) {
      return http.BUFFER_SIZE;
    } else if (http.getMaxContent() - totalRead < http.BUFFER_SIZE) {
      tryToRead = http.getMaxContent() - totalRead;
    }
    return tryToRead;
  }

}

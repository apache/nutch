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

// HTTP Client imports
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

// Nutch imports
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ContentProperties;


/**
 * An HTTP response.
 */
public class HttpResponse implements Response {

  private URL url;
  
  private String orig;

  private String base;

  private byte[] content;


  private int code;

  private ContentProperties headers = new ContentProperties();

  
  public HttpResponse(URL url, CrawlDatum datum) throws IOException {
    this(url, datum, false);
  }

  
  HttpResponse(URL url, CrawlDatum datum, boolean followRedirects) throws IOException {
    this.url = url;
    this.base = url.toString();
    this.orig = url.toString();
    GetMethod get = new GetMethod(this.orig);
    get.setFollowRedirects(followRedirects);
    get.setRequestHeader("User-Agent", Http.AGENT_STRING);
    HttpMethodParams params = get.getParams();
    // some servers cannot digest the new protocol
    params.setVersion(HttpVersion.HTTP_1_0);
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
        headers.setProperty(heads[i].getName(), heads[i].getValue());
      }
      // always read content. Sometimes content is useful to find a cause
      // for error.
      try {
        InputStream in = get.getResponseBodyAsStream();
        byte[] buffer = new byte[Http.BUFFER_SIZE];
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
    } catch (org.apache.commons.httpclient.ProtocolException pe) {
      pe.printStackTrace();
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
    return (String) headers.get(name);
  }
  
  public ContentProperties getHeaders() {
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
    if (Http.MAX_CONTENT <= 0) {
      return Http.BUFFER_SIZE;
    } else if (Http.MAX_CONTENT - totalRead < Http.BUFFER_SIZE) {
      tryToRead = Http.MAX_CONTENT - totalRead;
    }
    return tryToRead;
  }

}

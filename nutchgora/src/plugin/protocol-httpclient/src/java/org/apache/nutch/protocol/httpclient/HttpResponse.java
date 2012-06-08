/**
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
package org.apache.nutch.protocol.httpclient;

// JDK imports
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

// HTTP Client imports
import org.apache.avro.util.Utf8;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.HttpException;

// Nutch imports
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.nutch.storage.WebPage;

/**
 * An HTTP response.
 *
 * @author Susam Pal
 */
public class HttpResponse implements Response {

  private URL url;
  private byte[] content;
  private int code;
  private Metadata headers = new SpellCheckedMetadata();

  /**
   * Fetches the given <code>url</code> and prepares HTTP response.
   *
   * @param http                An instance of the implementation class
   *                            of this plugin
   * @param url                 URL to be fetched
   * @param page                WebPage
   * @param followRedirects     Whether to follow redirects; follows
   *                            redirect if and only if this is true
   * @return                    HTTP response
   * @throws IOException        When an error occurs
   */
  HttpResponse(Http http, URL url, WebPage page,
      boolean followRedirects) throws IOException {

    // Prepare GET method for HTTP request
    this.url = url;
    GetMethod get = new GetMethod(url.toString());
    get.setFollowRedirects(followRedirects);
    get.setDoAuthentication(true);
    if (page.getModifiedTime() > 0) {
      get.setRequestHeader("If-Modified-Since",
          HttpDateFormat.toString(page.getModifiedTime()));
    }

    // Set HTTP parameters
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
      
      // Limit download size
      int contentLength = Integer.MAX_VALUE;
      String contentLengthString = headers.get(Response.CONTENT_LENGTH);
      if (contentLengthString != null) {
        try {
          contentLength = Integer.parseInt(contentLengthString.trim());
        } catch (NumberFormatException ex) {
          throw new HttpException("bad content length: " +
              contentLengthString);
        }
      }
      if (http.getMaxContent() >= 0 &&
          contentLength > http.getMaxContent()) {
        contentLength = http.getMaxContent();
      }

      // always read content. Sometimes content is useful to find a cause
      // for error.
      InputStream in = get.getResponseBodyAsStream();
      try {
        byte[] buffer = new byte[HttpBase.BUFFER_SIZE];
        int bufferFilled = 0;
        int totalRead = 0;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while ((bufferFilled = in.read(buffer, 0, buffer.length)) != -1
            && totalRead + bufferFilled <= contentLength) {
          totalRead += bufferFilled;
          out.write(buffer, 0, bufferFilled);
        }

        content = out.toByteArray();
      } catch (Exception e) {
        if (code == 200) throw new IOException(e.toString());
        // for codes other than 200 OK, we are fine with empty content
      } finally {
        if (in != null) {
          in.close();
        }
        get.abort();
      }
      
      StringBuilder fetchTrace = null;
      if (Http.LOG.isTraceEnabled()) {
        // Trace message
        fetchTrace = new StringBuilder("url: " + url +
            "; status code: " + code +
            "; bytes received: " + content.length);
        if (getHeader(Response.CONTENT_LENGTH) != null)
          fetchTrace.append("; Content-Length: " +
              getHeader(Response.CONTENT_LENGTH));
        if (getHeader(Response.LOCATION) != null)
          fetchTrace.append("; Location: " + getHeader(Response.LOCATION));
      }
      // Extract gzip, x-gzip and deflate content
      if (content != null) {
        // check if we have to uncompress it
        String contentEncoding = headers.get(Response.CONTENT_ENCODING);
        if (contentEncoding != null && Http.LOG.isTraceEnabled())
          fetchTrace.append("; Content-Encoding: " + contentEncoding);
        if ("gzip".equals(contentEncoding) ||
            "x-gzip".equals(contentEncoding)) {
          content = http.processGzipEncoded(content, url);
          if (Http.LOG.isTraceEnabled())
            fetchTrace.append("; extracted to " + content.length + " bytes");
        } else if ("deflate".equals(contentEncoding)) {
          content = http.processDeflateEncoded(content, url);
          if (Http.LOG.isTraceEnabled())
            fetchTrace.append("; extracted to " + content.length + " bytes");
        }
      }
      
      // add headers in metadata to row
	  if (page.getHeaders() != null) {
	    page.getHeaders().clear();
	  }
	  for (String key : headers.names()) {
	    page.putToHeaders(new Utf8(key), new Utf8(headers.get(key)));
	  }

      // Logger trace message
      if (Http.LOG.isTraceEnabled()) {
        Http.LOG.trace(fetchTrace.toString());
      }
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
}


/* Copyright (c) 2004 The Nutch Organization.  All rights reserved.   */
/* Use subject to the conditions in http://www.nutch.org/LICENSE.txt. */

package org.apache.nutch.protocol.httpclient;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ContentProperties;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpVersion;

import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * An HTTP response.
 */
public class HttpResponse {

  private String orig;

  private String base;

  private byte[] content;

  private static final byte[] EMPTY_CONTENT = new byte[0];

  private int code;

  private ContentProperties headers = new ContentProperties();

  /**
   * Returns the response code.
   */
  public int getCode() {
    return code;
  }

  /**
   * Returns the value of a named header.
   */
  public String getHeader(String name) {
    return (String) headers.get(name);
  }

  public byte[] getContent() {
    return content;
  }

  public Content toContent() {
    return new Content(orig, base,
                       (content == null ? EMPTY_CONTENT : content),
                       getHeader("Content-Type"),
                       headers);
  }

  public HttpResponse(URL url, CrawlDatum datum) throws IOException {
    this(url, datum, false);
  }

  HttpResponse(URL url, CrawlDatum datum, boolean followRedirects) throws IOException {
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

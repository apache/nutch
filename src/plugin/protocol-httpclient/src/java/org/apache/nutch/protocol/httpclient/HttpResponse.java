/* Copyright (c) 2004 The Nutch Organization.  All rights reserved.   */
/* Use subject to the conditions in http://www.nutch.org/LICENSE.txt. */

package org.apache.nutch.protocol.httpclient;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolException;

import org.apache.commons.httpclient.Header;

import org.apache.commons.httpclient.methods.GetMethod;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.List;
import java.util.ListIterator;

/**
 * An HTTP response.
 */
public class HttpResponse {
  private String orig;

  private String base;

  private byte[] content;

  private int code;

  private MultiProperties headers = new MultiProperties();

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
    String contentType = getHeader("Content-Type");
    if (contentType == null) contentType = "";
    return new Content(orig, base, content, contentType, headers);
  }

  public HttpResponse(URL url) throws ProtocolException, IOException {
    this(url.toString(), url);
  }

  public HttpResponse(String orig, URL url) throws IOException {
    this.orig = orig;
    this.base = url.toString();
    GetMethod get = new GetMethod(url.toString());
    get.setFollowRedirects(false);
    get.setStrictMode(false);
    get.setRequestHeader("User-Agent", Http.AGENT_STRING);
    get.setHttp11(false);
    get.setMethodRetryHandler(null);
    try {
      code = Http.getClient().executeMethod(get);

      Header[] heads = get.getResponseHeaders();

      for (int i = 0; i < heads.length; i++) {
        headers.put(heads[i].getName(), heads[i].getValue());
      }
      if (code == 200) {

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

      }
    } catch (org.apache.commons.httpclient.ProtocolException pe) {
      pe.printStackTrace();
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

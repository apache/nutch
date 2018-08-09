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
package org.apache.nutch.protocol.interactiveselenium;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.PushbackInputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.http.api.HttpException;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.openqa.selenium.WebDriver;

import org.apache.nutch.protocol.selenium.HttpWebClient;
import org.apache.nutch.protocol.interactiveselenium.handlers.InteractiveSeleniumHandler;
/* Most of this code was borrowed from protocol-htmlunit; which in turn borrowed it from protocol-httpclient */

public class HttpResponse implements Response {

  private Http http;
  private URL url;
  private String orig;
  private String base;
  private byte[] content;
  private int code;
  private Metadata headers = new SpellCheckedMetadata();
  private InteractiveSeleniumHandler[] handlers;
  // used for storing the http headers verbatim
  private StringBuffer httpHeaders;

  protected enum Scheme {
    HTTP, HTTPS,
  }
  /** The nutch configuration */
  private Configuration conf = null;

  public HttpResponse(Http http, URL url, CrawlDatum datum) throws ProtocolException, IOException {

    this.conf = http.getConf();
    this.http = http;
    this.url = url;
    this.orig = url.toString();
    this.base = url.toString();
    Scheme scheme = null;

    if ("http".equals(url.getProtocol())) {
      scheme = Scheme.HTTP;
    } else if ("https".equals(url.getProtocol())) {
      scheme = Scheme.HTTPS;
    } else {
      throw new HttpException("Unknown scheme (not http/https) for url:" + url);
    }

    if (Http.LOG.isTraceEnabled()) {
      Http.LOG.trace("fetching " + url);
    }

    String path = "".equals(url.getFile()) ? "/" : url.getFile();

    // some servers will redirect a request with a host line like
    // "Host: <hostname>:80" to "http://<hpstname>/<orig_path>"- they
    // don't want the :80...

    String host = url.getHost();
    int port;
    String portString;
    if (url.getPort() == -1) {
      if (scheme == Scheme.HTTP) {
        port = 80;
      } else {
        port = 443;
      }
      portString = "";
    } else {
      port = url.getPort();
      portString = ":" + port;
    }
    Socket socket = null;

    try {
      socket = new Socket(); // create the socket
      socket.setSoTimeout(http.getTimeout());

      // connect
      String sockHost = http.useProxy(url) ? http.getProxyHost() : host;
      int sockPort = http.useProxy(url) ? http.getProxyPort() : port;
      InetSocketAddress sockAddr = new InetSocketAddress(sockHost, sockPort);
      socket.connect(sockAddr, http.getTimeout());

      if (scheme == Scheme.HTTPS) {
        SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory
                .getDefault();
        SSLSocket sslsocket = (SSLSocket) factory
                .createSocket(socket, sockHost, sockPort, true);
        sslsocket.setUseClientMode(true);

        // Get the protocols and ciphers supported by this JVM
        Set<String> protocols = new HashSet<String>(
                Arrays.asList(sslsocket.getSupportedProtocols()));
        Set<String> ciphers = new HashSet<String>(
                Arrays.asList(sslsocket.getSupportedCipherSuites()));

        // Intersect with preferred protocols and ciphers
        protocols.retainAll(http.getTlsPreferredProtocols());
        ciphers.retainAll(http.getTlsPreferredCipherSuites());

        sslsocket.setEnabledProtocols(
                protocols.toArray(new String[protocols.size()]));
        sslsocket.setEnabledCipherSuites(
                ciphers.toArray(new String[ciphers.size()]));

        sslsocket.startHandshake();
        socket = sslsocket;
      }

      if (sockAddr != null
              && conf.getBoolean("store.ip.address", false) == true) {
        headers.add("_ip_", sockAddr.getAddress().getHostAddress());
      }
      // make request
      OutputStream req = socket.getOutputStream();

      StringBuffer reqStr = new StringBuffer("GET ");
      if (http.useProxy(url)) {
        reqStr.append(url.getProtocol() + "://" + host + portString + path);
      } else {
        reqStr.append(path);
      }

      reqStr.append(" HTTP/1.0\r\n");

      reqStr.append("Host: ");
      reqStr.append(host);
      reqStr.append(portString);
      reqStr.append("\r\n");

      reqStr.append("Accept-Encoding: x-gzip, gzip, deflate\r\n");

      String userAgent = http.getUserAgent();
      if ((userAgent == null) || (userAgent.length() == 0)) {
        if (Http.LOG.isErrorEnabled()) {
          Http.LOG.error("User-agent is not set!");
        }
      } else {
        reqStr.append("User-Agent: ");
        reqStr.append(userAgent);
        reqStr.append("\r\n");
      }

      String acceptLanguage = http.getAcceptLanguage();
      if (!acceptLanguage.isEmpty()) {
        reqStr.append("Accept-Language: ");
        reqStr.append(acceptLanguage);
        reqStr.append("\r\n");
      }

      String acceptCharset = http.getAcceptCharset();
      if (!acceptCharset.isEmpty()) {
        reqStr.append("Accept-Charset: ");
        reqStr.append(acceptCharset);
        reqStr.append("\r\n");
      }

      String accept = http.getAccept();
      if (!accept.isEmpty()) {
        reqStr.append("Accept: ");
        reqStr.append(accept);
        reqStr.append("\r\n");
      }

      if (http.isCookieEnabled()
              && datum.getMetaData().containsKey(HttpBase.COOKIE)) {
        String cookie = ((Text) datum.getMetaData().get(HttpBase.COOKIE))
                .toString();
        reqStr.append("Cookie: ");
        reqStr.append(cookie);
        reqStr.append("\r\n");
      }

      if (http.isIfModifiedSinceEnabled() && datum.getModifiedTime() > 0) {
        reqStr.append("If-Modified-Since: " + HttpDateFormat
                .toString(datum.getModifiedTime()));
        reqStr.append("\r\n");
      }
      reqStr.append("\r\n");

      // store the request in the metadata?
      if (conf.getBoolean("store.http.request", false) == true) {
        headers.add("_request_", reqStr.toString());
      }


      byte[] reqBytes = reqStr.toString().getBytes();

      req.write(reqBytes);
      req.flush();

      PushbackInputStream in = // process response
          new PushbackInputStream(new BufferedInputStream(socket.getInputStream(), Http.BUFFER_SIZE),
              Http.BUFFER_SIZE);

      StringBuffer line = new StringBuffer();


      // store the http headers verbatim
      if (conf.getBoolean("store.http.headers", false) == true) {
        httpHeaders = new StringBuffer();
      }

      headers.add("nutch.fetch.time", Long.toString(System.currentTimeMillis()));

      boolean haveSeenNonContinueStatus = false;
      while (!haveSeenNonContinueStatus) {
        // parse status code line
        this.code = parseStatusLine(in, line);
        if (httpHeaders != null)
          httpHeaders.append(line).append("\n");
        // parse headers
        parseHeaders(in, line);
        haveSeenNonContinueStatus = code != 100; // 100 is "Continue"
      }

      // Get Content type header
      String contentType = getHeader(Response.CONTENT_TYPE);

      // handle with Selenium only if content type in HTML or XHTML 
      if (contentType != null) {
        if (contentType.contains("text/html") || contentType.contains("application/xhtml")) {
          readPlainContent(url);
        } else {
          try {
            int contentLength = Integer.MAX_VALUE;
            String contentLengthString = headers.get(Response.CONTENT_LENGTH);
            if (contentLengthString != null) {
              try {
                contentLength = Integer.parseInt(contentLengthString.trim());
              } catch (NumberFormatException ex) {
                throw new HttpException("bad content length: " + contentLengthString);
              }
            }

            if (http.getMaxContent() >= 0 && contentLength > http.getMaxContent()) {
              contentLength = http.getMaxContent();
            }

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
            if (code == 200)
              throw new IOException(e.toString());
            // for codes other than 200 OK, we are fine with empty content
          } finally {
            if (in != null) {
              in.close();
            }
          }
        }
      } 

    } finally {
      if (socket != null)
        socket.close();
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

  /* ------------------------- *
   * <implementation:Response> *
   * ------------------------- */
  private void loadSeleniumHandlers() {
    if (handlers != null) return;

    String handlerConfig = this.conf.get("interactiveselenium.handlers", "DefaultHandler");
    String[] handlerNames = handlerConfig.split(",");
    handlers = new InteractiveSeleniumHandler[handlerNames.length];
    for (int i = 0; i < handlerNames.length; i++) {
        try {
            String classToLoad = this.getClass().getPackage().getName() + ".handlers." + handlerNames[i];
            try {
              handlers[i] = InteractiveSeleniumHandler.class.cast(Class.forName(classToLoad).getConstructor().newInstance());
            } catch (IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
              e.printStackTrace();
            }
            Http.LOG.info("Successfully loaded " + classToLoad);
        } catch (ClassNotFoundException e) {
            Http.LOG.info("Unable to load Handler class for: " + handlerNames[i]);
        } catch (InstantiationException e) {
            Http.LOG.info("Unable to instantiate Handler: " + handlerNames[i]);
        } catch (IllegalAccessException e) {
            Http.LOG.info("Illegal access with Handler: " + handlerNames[i]);
        }
    }
  }

  private void readPlainContent(URL url) throws IOException {
    if (handlers == null)
        loadSeleniumHandlers();

    String processedPage = "";

    for (InteractiveSeleniumHandler handler : this.handlers) {
        if (! handler.shouldProcessURL(url.toString())) {
            continue;
        }

        WebDriver driver = HttpWebClient.getDriverForPage(url.toString(), conf);

        processedPage += handler.processDriver(driver);

        HttpWebClient.cleanUpDriver(driver);
    }

    content = processedPage.getBytes("UTF-8");
  }

  private int parseStatusLine(PushbackInputStream in, StringBuffer line) throws IOException, HttpException {
    readLine(in, line, false);

    int codeStart = line.indexOf(" ");
    int codeEnd = line.indexOf(" ", codeStart + 1);

    // handle lines with no plaintext result code, ie:
    // "HTTP/1.1 200" vs "HTTP/1.1 200 OK"
    if (codeEnd == -1)
      codeEnd = line.length();

    int code;
    try {
      code = Integer.parseInt(line.substring(codeStart + 1, codeEnd));
    } catch (NumberFormatException e) {
      throw new HttpException("bad status line '" + line + "': " + e.getMessage(), e);
    }

    return code;
  }

  private void processHeaderLine(StringBuffer line) throws IOException, HttpException {

    int colonIndex = line.indexOf(":"); // key is up to colon
    if (colonIndex == -1) {
      int i;
      for (i = 0; i < line.length(); i++)
        if (!Character.isWhitespace(line.charAt(i)))
          break;
      if (i == line.length())
        return;
      throw new HttpException("No colon in header:" + line);
    }
    String key = line.substring(0, colonIndex);

    int valueStart = colonIndex + 1; // skip whitespace
    while (valueStart < line.length()) {
      int c = line.charAt(valueStart);
      if (c != ' ' && c != '\t')
        break;
      valueStart++;
    }
    String value = line.substring(valueStart);
    headers.set(key, value);
  }

  // Adds headers to our headers Metadata
  private void parseHeaders(PushbackInputStream in, StringBuffer line) throws IOException, HttpException {

    while (readLine(in, line, true) != 0) {

      // handle HTTP responses with missing blank line after headers
      int pos;
      if (((pos = line.indexOf("<!DOCTYPE")) != -1) || ((pos = line.indexOf("<HTML")) != -1)
          || ((pos = line.indexOf("<html")) != -1)) {

        in.unread(line.substring(pos).getBytes("UTF-8"));
        line.setLength(pos);

        try {
          //TODO: (CM) We don't know the header names here
          //since we're just handling them generically. It would
          //be nice to provide some sort of mapping function here
          //for the returned header names to the standard metadata
          //names in the ParseData class
          processHeaderLine(line);
        } catch (Exception e) {
          // fixme:
          Http.LOG.warn("Error: ", e);
        }
        return;
      }

      processHeaderLine(line);
    }
  }

  private static int readLine(PushbackInputStream in, StringBuffer line, boolean allowContinuedLine)
      throws IOException {
    line.setLength(0);
    for (int c = in.read(); c != -1; c = in.read()) {
      switch (c) {
      case '\r':
        if (peek(in) == '\n') {
          in.read();
        }
      case '\n':
        if (line.length() > 0) {
          // at EOL -- check for continued line if the current
          // (possibly continued) line wasn't blank
          if (allowContinuedLine)
            switch (peek(in)) {
            case ' ':
            case '\t': // line is continued
              in.read();
              continue;
            }
        }
        return line.length(); // else complete
      default:
        line.append((char) c);
      }
    }
    throw new EOFException();
  }

  private static int peek(PushbackInputStream in) throws IOException {
    int value = in.read();
    in.unread(value);
    return value;
  }
}

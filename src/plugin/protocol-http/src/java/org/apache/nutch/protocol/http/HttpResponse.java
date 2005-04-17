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

package org.apache.nutch.protocol.http;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.util.Map;
import java.util.TreeMap;
import java.util.Properties;
import java.util.logging.Level;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolException;

import org.apache.nutch.util.GZIPUtils;
import org.apache.nutch.util.NutchConf;
import org.apache.nutch.util.mime.MimeType;
import org.apache.nutch.util.mime.MimeTypes;


/** An HTTP response. */
public class HttpResponse {

  /** A flag that tells if magic resolution must be performed */
  private final static boolean MAGIC =
        NutchConf.get().getBoolean("mime.type.magic", true);

  /** Get the MimeTypes resolver instance. */
  private final static MimeTypes MIME = 
        MimeTypes.get(NutchConf.get().get("mime.types.file"));

  
  private String orig;
  private String base;
  private byte[] content;
  private int code;
  private Properties headers = new Properties();

  /** Returns the response code. */
  public int getCode() { return code; }

  /** Returns the value of a named header. */
  public String getHeader(String name) {
    return (String)headers.get(name);
  }

  public byte[] getContent() { return content; }

  public Content toContent() {
    String contentType = getHeader("Content-Type");
    if (contentType == null) {
      MimeType type = null;
      if (MAGIC) {
        type = MIME.getMimeType(orig, content);
      } else {
        type = MIME.getMimeType(orig);
      }
      if (type != null) {
          contentType = type.getName();
      } else {
          contentType = "";
      }
    }
    return new Content(orig, base, content, contentType, headers);
  }

  public HttpResponse(URL url) throws ProtocolException, IOException {
    this(url.toString(), url);
  }

  public HttpResponse(String orig, URL url)
    throws ProtocolException, IOException {
    
    this.orig = orig;
    this.base = url.toString();

    if (!"http".equals(url.getProtocol()))
      throw new HttpException("Not an HTTP url:" + url);

    if (Http.LOG.isLoggable(Level.FINE))
      Http.LOG.fine("fetching " + url);

    String path = "".equals(url.getFile()) ? "/" : url.getFile();

    // some servers will redirect a request with a host line like
    // "Host: <hostname>:80" to "http://<hpstname>/<orig_path>"- they
    // don't want the :80...

    String host = url.getHost();
    int port;
    String portString;
    if (url.getPort() == -1) {
      port= 80;
      portString= "";
    } else {
      port= url.getPort();
      portString= ":" + port;
    }
    Socket socket = null;

    try {
      socket = new Socket();                    // create the socket
      socket.setSoTimeout(Http.TIMEOUT);


      // connect
      String sockHost = Http.PROXY ? Http.PROXY_HOST : host;
      int sockPort = Http.PROXY ? Http.PROXY_PORT : port;
      InetSocketAddress sockAddr= new InetSocketAddress(sockHost, sockPort);
      socket.connect(sockAddr, Http.TIMEOUT);

      // make request
      OutputStream req = socket.getOutputStream();

      StringBuffer reqStr = new StringBuffer("GET ");
      if(Http.PROXY){
      	reqStr.append(url.getProtocol()+"://"+host+portString+path);
      } else {
      	reqStr.append(path);
      }

      reqStr.append(" HTTP/1.0\r\n");

      reqStr.append("Host: ");
      reqStr.append(host);
      reqStr.append(portString);
      reqStr.append("\r\n");

      reqStr.append("Accept-Encoding: x-gzip, gzip\r\n");

      if ((Http.AGENT_STRING == null) || (Http.AGENT_STRING.length() == 0)) {
        Http.LOG.severe("User-agent is not set!");
      } else {
        reqStr.append("User-Agent: ");
        reqStr.append(Http.AGENT_STRING);
        reqStr.append("\r\n");
      }

      reqStr.append("\r\n");
      byte[] reqBytes= reqStr.toString().getBytes();

      req.write(reqBytes);
      req.flush();
        
      PushbackInputStream in =                  // process response
        new PushbackInputStream(
          new BufferedInputStream(socket.getInputStream(), Http.BUFFER_SIZE), 
          Http.BUFFER_SIZE) ;

      StringBuffer line = new StringBuffer();

      boolean haveSeenNonContinueStatus= false;
      while (!haveSeenNonContinueStatus) {
        // parse status code line
        this.code = parseStatusLine(in, line); 
        // parse headers
        headers.putAll(parseHeaders(in, line));
        haveSeenNonContinueStatus= code != 100; // 100 is "Continue"
      }

      readPlainContent(in);

      String contentEncoding= getHeader("Content-Encoding");
      if ("gzip".equals(contentEncoding) || "x-gzip".equals(contentEncoding)) {
        Http.LOG.fine("uncompressing....");
        byte[] compressed = content;

        content = GZIPUtils.unzipBestEffort(compressed, Http.MAX_CONTENT);

        if (content == null)
          throw new HttpException("unzipBestEffort returned null");

        if (Http.LOG.isLoggable(Level.FINE))
          Http.LOG.fine("fetched " + compressed.length
                        + " bytes of compressed content (expanded to "
                        + content.length + " bytes) from " + url);
      } else {
        if (Http.LOG.isLoggable(Level.FINE))
          Http.LOG.fine("fetched " + content.length + " bytes from " + url);
      }

    } finally {
      if (socket != null)
        socket.close();
    }

  }

  private void readPlainContent(InputStream in) 
    throws HttpException, IOException {

    int contentLength = Integer.MAX_VALUE;    // get content length
    String contentLengthString = (String)headers.get("Content-Length");
    if (contentLengthString != null) {
      contentLengthString = contentLengthString.trim();
      try {
        contentLength = Integer.parseInt(contentLengthString);
      } catch (NumberFormatException e) {
        throw new HttpException("bad content length: "+contentLengthString);
      }
    }
    if (Http.MAX_CONTENT >= 0
      && contentLength > Http.MAX_CONTENT)   // limit download size
      contentLength  = Http.MAX_CONTENT;

    ByteArrayOutputStream out = new ByteArrayOutputStream(Http.BUFFER_SIZE);
    byte[] bytes = new byte[Http.BUFFER_SIZE];
    int length = 0;                           // read content
    for (int i = in.read(bytes); i != -1; i = in.read(bytes)) {

      out.write(bytes, 0, i);
      length += i;
      if (length >= contentLength)
        break;
    }
    content = out.toByteArray();
  }

  private void readChunkedContent(PushbackInputStream in,  
                                  StringBuffer line) 
    throws HttpException, IOException {
    boolean doneChunks= false;
    int contentBytesRead= 0;
    byte[] bytes = new byte[Http.BUFFER_SIZE];
    ByteArrayOutputStream out = new ByteArrayOutputStream(Http.BUFFER_SIZE);

    while (!doneChunks) {
      Http.LOG.fine("Http: starting chunk");

      readLine(in, line, false);

      String chunkLenStr;
      // LOG.fine("chunk-header: '" + line + "'");

      int pos= line.indexOf(";");
      if (pos < 0) {
        chunkLenStr= line.toString();
      } else {
        chunkLenStr= line.substring(0, pos);
        // LOG.fine("got chunk-ext: " + line.substring(pos+1));
      }
      chunkLenStr= chunkLenStr.trim();
      int chunkLen;
      try {
        chunkLen= Integer.parseInt(chunkLenStr, 16);
      } catch (NumberFormatException e){ 
        throw new HttpException("bad chunk length: "+line.toString());
      }

      if (chunkLen == 0) {
        doneChunks= true;
        break;
      }

      if ( (contentBytesRead + chunkLen) > Http.MAX_CONTENT )
        chunkLen= Http.MAX_CONTENT - contentBytesRead;

      // read one chunk
      int chunkBytesRead= 0;
      while (chunkBytesRead < chunkLen) {

        int toRead= (chunkLen - chunkBytesRead) < Http.BUFFER_SIZE ?
                    (chunkLen - chunkBytesRead) : Http.BUFFER_SIZE;
        int len= in.read(bytes, 0, toRead);

        if (len == -1) 
          throw new HttpException("chunk eof after " + contentBytesRead
                                      + " bytes in successful chunks"
                                      + " and " + chunkBytesRead 
                                      + " in current chunk");

        // DANGER!!! Will printed GZIPed stuff right to your
        // terminal!
        // LOG.fine("read: " +  new String(bytes, 0, len));

        out.write(bytes, 0, len);
        chunkBytesRead+= len;  
      }

      readLine(in, line, false);

    }

    if (!doneChunks) {
      if (contentBytesRead != Http.MAX_CONTENT) 
        throw new HttpException("chunk eof: !doneChunk && didn't max out");
      return;
    }

    content = out.toByteArray();
    parseHeaders(in, line);

  }

  private int parseStatusLine(PushbackInputStream in, StringBuffer line)
    throws IOException, HttpException {
    readLine(in, line, false);

    int codeStart = line.indexOf(" ");
    int codeEnd = line.indexOf(" ", codeStart+1);

    // handle lines with no plaintext result code, ie:
    // "HTTP/1.1 200" vs "HTTP/1.1 200 OK"
    if (codeEnd == -1) 
      codeEnd= line.length();

    int code;
    try {
      code= Integer.parseInt(line.substring(codeStart+1, codeEnd));
    } catch (NumberFormatException e) {
      throw new HttpException("bad status line '" + line 
                              + "': " + e.getMessage(), e);
    }

    return code;
  }


  private void processHeaderLine(StringBuffer line, TreeMap headers)
    throws IOException, HttpException {
    int colonIndex = line.indexOf(":");       // key is up to colon
    if (colonIndex == -1) {
      int i;
      for (i= 0; i < line.length(); i++)
        if (!Character.isWhitespace(line.charAt(i)))
          break;
      if (i == line.length())
        return;
      throw new HttpException("No colon in header:" + line);
    }
    String key = line.substring(0, colonIndex);

    int valueStart = colonIndex+1;            // skip whitespace
    while (valueStart < line.length()) {
      int c = line.charAt(valueStart);
      if (c != ' ' && c != '\t')
        break;
      valueStart++;
    }
    String value = line.substring(valueStart);

    headers.put(key, value);
  }

  private Map parseHeaders(PushbackInputStream in, StringBuffer line)
    throws IOException, HttpException {
    TreeMap headers = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    return parseHeaders(in, line, headers);
  }

  // Adds headers to an existing TreeMap
  private Map parseHeaders(PushbackInputStream in, StringBuffer line,
                           TreeMap headers)
    throws IOException, HttpException {
    while (readLine(in, line, true) != 0) {

      // handle HTTP responses with missing blank line after headers
      int pos;
      if ( ((pos= line.indexOf("<!DOCTYPE")) != -1) 
           || ((pos= line.indexOf("<HTML")) != -1) 
           || ((pos= line.indexOf("<html")) != -1) ) {

        in.unread(line.substring(pos).getBytes("UTF-8"));
        line.setLength(pos);

        try {
          processHeaderLine(line, headers);
        } catch (Exception e) {
          // fixme:
          e.printStackTrace();
        }

        return headers;
      }

      processHeaderLine(line, headers);
    }
    return headers;
  }

  private static int readLine(PushbackInputStream in, StringBuffer line,
                      boolean allowContinuedLine)
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
                case ' ' : case '\t':                   // line is continued
                  in.read();
                  continue;
              }
          }
          return line.length();      // else complete
        default :
          line.append((char)c);
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

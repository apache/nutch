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

package org.apache.nutch.protocol.file;

// JDK imports
import java.net.URL;
import java.util.Date;
import java.util.TreeMap;
import java.io.IOException;

// Nutch imports
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.net.protocols.Response;

// Tika imports
import org.apache.tika.mime.MimeType;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;


/************************************
 * FileResponse.java mimics file replies as http response.
 * It tries its best to follow http's way for headers, response codes
 * as well as exceptions.
 *
 * Comments:
 * (1) java.net.URL and java.net.URLConnection can handle file: scheme.
 * However they are not flexible enough, so not used in this implementation.
 *
 * (2) java.io.File is used for its abstractness across platforms.
 * Warning:
 * java.io.File API (1.4.2) does not elaborate on how special files,
 * such as /dev/* in unix and /proc/* on linux, are treated. Tests show
 *  (a) java.io.File.isFile() return false for /dev/*
 *  (b) java.io.File.isFile() return true for /proc/*
 *  (c) java.io.File.length() return 0 for /proc/*
 * We are probably oaky for now. Could be buggy here.
 * How about special files on windows?
 *
 * (3) java.io.File API (1.4.2) does not seem to know unix hard link files.
 * They are just treated as individual files.
 *
 * (4) No funcy POSIX file attributes yet. May never need?
 *
 * @author John Xing
 ***********************************/
public class FileResponse {

  private String orig;
  private String base;
  private byte[] content;
  private static final byte[] EMPTY_CONTENT = new byte[0];
  private int code;
  private Metadata headers = new Metadata();

  private final File file;
  private Configuration conf;
  
  private MimeUtil MIME;

  /** Returns the response code. */
  public int getCode() { return code; }

  /** Returns the value of a named header. */
  public String getHeader(String name) {
    return headers.get(name);
  }

  public byte[] getContent() { return content; }

  public Content toContent() {
    return new Content(orig, base, (content != null ? content : EMPTY_CONTENT),
                       getHeader(Response.CONTENT_TYPE),
                       headers, this.conf);
  }
  
  public FileResponse(URL url, CrawlDatum datum, File file, Configuration conf)
    throws FileException, IOException {

    this.orig = url.toString();
    this.base = url.toString();
    this.file = file;
    this.conf = conf;
    
    MIME = new MimeUtil(conf);

    if (!"file".equals(url.getProtocol()))
      throw new FileException("Not a file url:" + url);

    if (File.LOG.isTraceEnabled()) {
      File.LOG.trace("fetching " + url);
    }

    if (url.getPath() != url.getFile()) {
      if (File.LOG.isWarnEnabled()) {
        File.LOG.warn("url.getPath() != url.getFile(): " + url);
      }
    }

    String path = "".equals(url.getPath()) ? "/" : url.getPath();

    try {

      this.content = null;

      // url.toURI() is only in j2se 1.5.0
      //java.io.File f = new java.io.File(url.toURI());
      java.io.File f = new java.io.File(path);

      if (!f.exists()) {
        this.code = 404;  // http Not Found
        return;
      }

      if (!f.canRead()) {
        this.code = 401;  // http Unauthorized
        return;
      }

      // symbolic link or relative path on unix
      // fix me: what's the consequence on windows platform
      // where case is insensitive
      if (!f.equals(f.getCanonicalFile())) {
        // set headers
        //hdrs.put("Location", f.getCanonicalFile().toURI());
        headers.set(Response.LOCATION, f.getCanonicalFile().toURL().toString());

        this.code = 300;  // http redirect
        return;
      }
      if (f.lastModified() <= datum.getModifiedTime()) {
        this.code = 304;
        this.headers.set("Last-Modified", HttpDateFormat.toString(f.lastModified()));
        return;
      }

      if (f.isDirectory()) {
        getDirAsHttpResponse(f);
      } else if (f.isFile()) {
        getFileAsHttpResponse(f);
      } else {
        this.code = 500; // http Internal Server Error
        return;
      }

    } catch (IOException e) {
      throw e;
    }

  }

  // get file as http response
  private void getFileAsHttpResponse(java.io.File f)
    throws FileException, IOException {

    // ignore file of size larger than
    // Integer.MAX_VALUE = 2^31-1 = 2147483647
    long size = f.length();
    if (size > Integer.MAX_VALUE) {
      throw new FileException("file is too large, size: "+size);
      // or we can do this?
      // this.code = 400;  // http Bad request
      // return;
    }

    // capture content
    int len = (int) size;
    
    if (this.file.maxContentLength >= 0 && len > this.file.maxContentLength)
      len = this.file.maxContentLength;

    this.content = new byte[len];

    java.io.InputStream is = new java.io.FileInputStream(f);
    int offset = 0; int n = 0;
    while (offset < len
      && (n = is.read(this.content, offset, len-offset)) >= 0) {
      offset += n;
    }
    if (offset < len) { // keep whatever already have, but issue a warning
      if (File.LOG.isWarnEnabled()) {
        File.LOG.warn("not enough bytes read from file: "+f.getPath());
      }
    }
    is.close(); 

    // set headers
    headers.set(Response.CONTENT_LENGTH, new Long(size).toString());
    headers.set(Response.LAST_MODIFIED, HttpDateFormat.toString(f
        .lastModified()));
    
    MimeType mimeType = MIME.getMimeType(f);
    String mimeTypeString = mimeType != null ? mimeType.getName() : "";
    headers.set(Response.CONTENT_TYPE, mimeTypeString);

    // response code
    this.code = 200; // http OK
  }

  // get dir list as http response
  private void getDirAsHttpResponse(java.io.File f)
    throws IOException {

    String path = f.toString();
    this.content = list2html(f.listFiles(), path, "/".equals(path) ? false : true);

    // set headers
    headers.set(Response.CONTENT_LENGTH,
      new Integer(this.content.length).toString());
    headers.set(Response.CONTENT_TYPE, "text/html");
    headers.set(Response.LAST_MODIFIED,
      HttpDateFormat.toString(f.lastModified()));

    // response code
    this.code = 200; // http OK
  }

  // generate html page from dir list
  private byte[] list2html(java.io.File[] list,
    String path, boolean includeDotDot) {

    StringBuffer x = new StringBuffer("<html><head>");
    x.append("<title>Index of "+path+"</title></head>\n");
    x.append("<body><h1>Index of "+path+"</h1><pre>\n");

    if (includeDotDot) {
      x.append("<a href='../'>../</a>\t-\t-\t-\n");
    }

    // fix me: we might want to sort list here! but not now.

    java.io.File f;
    for (int i=0; i<list.length; i++) {
      f = list[i];
      String name = f.getName();
      String time = HttpDateFormat.toString(f.lastModified());
      if (f.isDirectory()) {
        // java 1.4.2 api says dir itself and parent dir are not listed
        // so the following is not needed.
        //if (name.equals(".") || name.equals(".."))
        //  continue;
        x.append("<a href='"+name+"/"+"'>"+name+"/</a>\t");
        x.append(time+"\t-\n");
      } else if (f.isFile()) {
        x.append("<a href='"+name+    "'>"+name+"</a>\t");
        x.append(time+"\t"+f.length()+"\n");
      } else {
        // ignore any other
      }
    }

    x.append("</pre></body></html>\n");

    return new String(x).getBytes();
  }

}

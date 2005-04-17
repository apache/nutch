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

package org.apache.nutch.protocol.file;

// JDK imports
import java.net.URL;
import java.util.TreeMap;
import java.util.Properties;
import java.util.logging.Level;
import java.io.IOException;

// Nutch imports
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConf;
import org.apache.nutch.util.mime.MimeType;
import org.apache.nutch.util.mime.MimeTypes;


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

  private final File file;

  /** Returns the response code. */
  public int getCode() { return code; }

  /** Returns the value of a named header. */
  public String getHeader(String name) {
    return (String)headers.get(name);
  }

  public byte[] getContent() { return content; }

  public Content toContent() {
    return new Content(orig, base, content,
                       getHeader("Content-Type"),
                       headers);
  }

  public FileResponse(URL url, File file)
    throws FileException, IOException {
    this(url.toString(), url, file);
  }

  public FileResponse(String orig, URL url, File file)
    throws FileException, IOException {

    this.orig = orig;
    this.base = url.toString();
    this.file = file;

    if (!"file".equals(url.getProtocol()))
      throw new FileException("Not a file url:" + url);

    if (File.LOG.isLoggable(Level.FINE))
      File.LOG.fine("fetching " + url);

    if (url.getPath() != url.getFile())
      File.LOG.warning("url.getPath() != url.getFile(): " + url);

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
        TreeMap hdrs = new TreeMap(String.CASE_INSENSITIVE_ORDER);
        //hdrs.put("Location", f.getCanonicalFile().toURI());
        hdrs.put("Location", f.getCanonicalFile().toURL().toString());
        this.headers.putAll(hdrs);

        this.code = 300;  // http redirect
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
    
    if (this.file.maxContentLength > 0 && len > this.file.maxContentLength)
      len = this.file.maxContentLength;

    this.content = new byte[len];

    java.io.InputStream is = new java.io.FileInputStream(f);
    int offset = 0; int n = 0;
    while (offset < len
      && (n = is.read(this.content, offset, len-offset)) >= 0) {
      offset += n;
    }
    if (offset < len) // keep whatever already have, but issue a warning
      File.LOG.warning("not enough bytes read from file: "+f.getPath());
    is.close(); 

    // set headers
    TreeMap hdrs = new TreeMap(String.CASE_INSENSITIVE_ORDER);

    hdrs.put("Content-Length", new Long(size).toString());

    hdrs.put("Last-Modified",
      this.file.httpDateFormat.toString(f.lastModified()));

    MimeType contentType = null;
    if (MAGIC) {
      contentType = MIME.getMimeType(f.getName(), this.content);
    } else {
      contentType = MIME.getMimeType(f.getName());
    }
    if (contentType != null) {
        hdrs.put("Content-Type", contentType.getName());
    }
    this.headers.putAll(hdrs);

    // response code
    this.code = 200; // http OK
  }

  // get dir list as http response
  private void getDirAsHttpResponse(java.io.File f)
    throws IOException {

    String path = f.toString();
    this.content = list2html(f.listFiles(), path, "/".equals(path) ? false : true);

    // set headers
    TreeMap hdrs = new TreeMap(String.CASE_INSENSITIVE_ORDER);

    hdrs.put("Content-Length",
      new Integer(this.content.length).toString());

    hdrs.put("Content-Type", "text/html");

    hdrs.put("Last-Modified",
      this.file.httpDateFormat.toString(f.lastModified()));

    this.headers.putAll(hdrs);

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
      String time = this.file.httpDateFormat.toString(f.lastModified());
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

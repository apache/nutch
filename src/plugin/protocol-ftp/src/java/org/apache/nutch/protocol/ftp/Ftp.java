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

package org.apache.nutch.protocol.ftp;


import org.apache.commons.net.ftp.FTPFileEntryParser;

import org.apache.nutch.db.Page;
import org.apache.nutch.net.protocols.HttpDateFormat;

import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.util.NutchConf;

import org.apache.nutch.pagedb.FetchListEntry;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.net.MalformedURLException;
import java.net.URL;

import java.io.IOException;

/************************************
 * Ftp.java deals with ftp: scheme.
 *
 * Configurable parameters are defined under "FTP properties" section
 * in ./conf/nutch-default.xml or similar.
 *
 * @author John Xing
 ***********************************/
public class Ftp implements Protocol {

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.protocol.ftp.Ftp");

  static final int BUFFER_SIZE = 16384; // 16*1024 = 16384

  static final int MAX_REDIRECTS = 5;

  static int timeout = NutchConf.get().getInt("ftp.timeout", 10000);

  static int maxContentLength = NutchConf.get().getInt("ftp.content.limit",64*1024);

  String userName = NutchConf.get().get("ftp.username", "anonymous");
  String passWord = NutchConf.get().get("ftp.password", "anonymous@example.com");

  // typical/default server timeout is 120*1000 millisec.
  // better be conservative here
  int serverTimeout = NutchConf.get().getInt("ftp.server.timeout", 60*1000);

  // when to have client start anew
  long renewalTime = -1;

  boolean keepConnection = NutchConf.get().getBoolean("ftp.keep.connection", false);

  boolean followTalk = NutchConf.get().getBoolean("ftp.follow.talk", false);

  // ftp client
  Client client = null;
  // ftp dir list entry parser
  FTPFileEntryParser parser = null;

  // 20040412, xing
  // the following three: HttpDateFormat, MimetypesFileTypeMap, MagicFile
  // are placed in each thread before we check out if they're thread-safe.

  // http date format
  HttpDateFormat httpDateFormat = null;


  // constructor
  public Ftp() {
    this.httpDateFormat = new HttpDateFormat();
  }

  /** Set the timeout. */
  public void setTimeout(int to) {
    timeout = to;
  }

  /** Set the point at which content is truncated. */
  public void setMaxContentLength(int length) {
    maxContentLength = length;
  }

  /** Set followTalk */
  public void setFollowTalk(boolean followTalk) {
    this.followTalk = followTalk;
  }

  /** Set keepConnection */
  public void setKeepConnection(boolean keepConnection) {
    this.keepConnection = keepConnection;
  }

  public ProtocolOutput getProtocolOutput(String urlString) {
    ProtocolOutput output = null;
    try {
      return getProtocolOutput(new FetchListEntry(true,
            new Page(urlString, 1.0f), new String[0]));
    } catch (MalformedURLException mue) {
      return new ProtocolOutput(null, new ProtocolStatus(mue));
    }
  }
  
  public ProtocolOutput getProtocolOutput(FetchListEntry fle) {
    String urlString = fle.getUrl().toString();
    try {
      URL url = new URL(urlString);
  
      int redirects = 0;
  
      while (true) {
        FtpResponse response;
        response = new FtpResponse(urlString, url, this);   // make a request
  
        int code = response.getCode();
  
        if (code == 200) {                          // got a good response
          return new ProtocolOutput(response.toContent());              // return it
  
        } else if (code >= 300 && code < 400) {     // handle redirect
          if (redirects == MAX_REDIRECTS)
            throw new FtpException("Too many redirects: " + url);
          url = new URL(response.getHeader("Location"));
          redirects++;                
          if (LOG.isLoggable(Level.FINE))
            LOG.fine("redirect to " + url); 
  
        } else {                                    // convert to exception
          throw new FtpError(code);
        }
      } 
    } catch (Exception e) {
      return new ProtocolOutput(null, new ProtocolStatus(e));
    }
  }

  protected void finalize () {
    try {
      if (this.client != null && this.client.isConnected()) {
        this.client.logout();
        this.client.disconnect();
      }
    } catch (IOException e) {
      // do nothing
    }
  }

  /** For debugging. */
  public static void main(String[] args) throws Exception {
    int timeout = Integer.MIN_VALUE;
    int maxContentLength = Integer.MIN_VALUE;
    String logLevel = "info";
    boolean followTalk = false;
    boolean keepConnection = false;
    boolean dumpContent = false;
    String urlString = null;

    String usage = "Usage: Ftp [-logLevel level] [-followTalk] [-keepConnection] [-timeout N] [-maxContentLength L] [-dumpContent] url";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }
      
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-logLevel")) {
        logLevel = args[++i];
      } else if (args[i].equals("-followTalk")) {
        followTalk = true;
      } else if (args[i].equals("-keepConnection")) {
        keepConnection = true;
      } else if (args[i].equals("-timeout")) {
        timeout = Integer.parseInt(args[++i]) * 1000;
      } else if (args[i].equals("-maxContentLength")) {
        maxContentLength = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-dumpContent")) {
        dumpContent = true;
      } else if (i != args.length-1) {
        System.err.println(usage);
        System.exit(-1);
      } else {
        urlString = args[i];
      }
    }

    Ftp ftp = new Ftp();

    ftp.setFollowTalk(followTalk);
    ftp.setKeepConnection(keepConnection);

    if (timeout != Integer.MIN_VALUE) // set timeout
      ftp.setTimeout(timeout);

    if (maxContentLength != Integer.MIN_VALUE) // set maxContentLength
      ftp.setMaxContentLength(maxContentLength);

    // set log level
    LOG.setLevel(Level.parse((new String(logLevel)).toUpperCase()));

    Content content = ftp.getProtocolOutput(urlString).getContent();

    System.err.println("Content-Type: " + content.getContentType());
    System.err.println("Content-Length: " + content.get("Content-Length"));
    System.err.println("Last-Modified: " + content.get("Last-Modified"));
    if (dumpContent) {
      System.out.print(new String(content.getContent()));
    }

    ftp = null;
  }

}

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

package org.apache.nutch.protocol.ftp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.net.ftp.FTPFileEntryParser;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.hadoop.io.Text;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.net.protocols.Response;

import org.apache.hadoop.conf.Configuration;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.EmptyRobotRules;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.protocol.RobotRules;

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

  public static final Log LOG = LogFactory.getLog(Ftp.class);

  static final int BUFFER_SIZE = 16384; // 16*1024 = 16384

  static final int MAX_REDIRECTS = 5;

  int timeout;

  int maxContentLength;

  String userName;
  String passWord; 

  // typical/default server timeout is 120*1000 millisec.
  // better be conservative here
  int serverTimeout;

  // when to have client start anew
  long renewalTime = -1;

  boolean keepConnection;

  boolean followTalk;

  // ftp client
  Client client = null;
  // ftp dir list entry parser
  FTPFileEntryParser parser = null;

  private Configuration conf;


  // constructor
  public Ftp() {
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

  public ProtocolOutput getProtocolOutput(Text url, CrawlDatum datum) {
    String urlString = url.toString();
    try {
      URL u = new URL(urlString);
  
      int redirects = 0;
  
      while (true) {
        FtpResponse response;
        response = new FtpResponse(u, datum, this, getConf());   // make a request
  
        int code = response.getCode();
  
        if (code == 200) {                          // got a good response
          return new ProtocolOutput(response.toContent());              // return it
  
        } else if (code >= 300 && code < 400) {     // handle redirect
          if (redirects == MAX_REDIRECTS)
            throw new FtpException("Too many redirects: " + url);
          u = new URL(response.getHeader("Location"));
          redirects++;                
          if (LOG.isTraceEnabled()) {
            LOG.trace("redirect to " + u); 
          }
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
    //LOG.setLevel(Level.parse((new String(logLevel)).toUpperCase()));

    Content content = ftp.getProtocolOutput(new Text(urlString), new CrawlDatum()).getContent();

    System.err.println("Content-Type: " + content.getContentType());
    System.err.println("Content-Length: " +
                       content.getMetadata().get(Response.CONTENT_LENGTH));
    System.err.println("Last-Modified: " +
                      content.getMetadata().get(Response.LAST_MODIFIED));
    if (dumpContent) {
      System.out.print(new String(content.getContent()));
    }

    ftp = null;
  }

  
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.maxContentLength = conf.getInt("ftp.content.limit", 64 * 1024);
    this.timeout = conf.getInt("ftp.timeout", 10000);
    this.userName = conf.get("ftp.username", "anonymous");
    this.passWord = conf.get("ftp.password", "anonymous@example.com");
    this.serverTimeout = conf.getInt("ftp.server.timeout", 60 * 1000);
    this.keepConnection = conf.getBoolean("ftp.keep.connection", false);
    this.followTalk = conf.getBoolean("ftp.follow.talk", false);
  }

  public Configuration getConf() {
    return this.conf;
  }

  public RobotRules getRobotRules(Text url, CrawlDatum datum) {
    return EmptyRobotRules.RULES;
  }

}

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.hadoop.io.Text;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.net.protocols.Response;

import org.apache.hadoop.conf.Configuration;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.EmptyRobotRules;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.protocol.RobotRules;
import org.apache.nutch.util.NutchConfiguration;

import java.net.URL;

/************************************
 * File.java deals with file: scheme.
 *
 * Configurable parameters are defined under "FILE properties" section
 * in ./conf/nutch-default.xml or similar.
 *
 * @author John Xing
 ***********************************/
public class File implements Protocol {

  public static final Log LOG = LogFactory.getLog(File.class);

  static final int MAX_REDIRECTS = 5;

  int maxContentLength;

  private Configuration conf;

  // constructor
  public File() {
  }

  /** Set the point at which content is truncated. */
  public void setMaxContentLength(int length) {maxContentLength = length;}

  public ProtocolOutput getProtocolOutput(Text url, CrawlDatum datum) {
    String urlString = url.toString();
    try {
      URL u = new URL(urlString);
  
      int redirects = 0;
  
      while (true) {
        FileResponse response;
        response = new FileResponse(u, datum, this, getConf());   // make a request
  
        int code = response.getCode();
  
        if (code == 200) {                          // got a good response
          return new ProtocolOutput(response.toContent());              // return it
  
        } else if (code >= 300 && code < 400) {     // handle redirect
          if (redirects == MAX_REDIRECTS)
            throw new FileException("Too many redirects: " + url);
          u = new URL(response.getHeader("Location"));
          redirects++;                
          if (LOG.isTraceEnabled()) {
            LOG.trace("redirect to " + u); 
          }
  
        } else {                                    // convert to exception
          throw new FileError(code);
        }
      } 
    } catch (Exception e) {
      e.printStackTrace();
      return new ProtocolOutput(null, new ProtocolStatus(e));
    }
  }

//  protected void finalize () {
//    // nothing here
//  }

  /** For debugging. */
  public static void main(String[] args) throws Exception {
    int maxContentLength = Integer.MIN_VALUE;
    String logLevel = "info";
    boolean dumpContent = false;
    String urlString = null;

    String usage = "Usage: File [-logLevel level] [-maxContentLength L] [-dumpContent] url";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }
      
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-logLevel")) {
        logLevel = args[++i];
      } else if (args[i].equals("-maxContentLength")) {
        maxContentLength = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-dumpContent")) {
        dumpContent = true;
      } else if (i != args.length-1) {
        System.err.println(usage);
        System.exit(-1);
      } else
        urlString = args[i];
    }

    File file = new File();
    file.setConf(NutchConfiguration.create());

    if (maxContentLength != Integer.MIN_VALUE) // set maxContentLength
      file.setMaxContentLength(maxContentLength);

    // set log level
    //LOG.setLevel(Level.parse((new String(logLevel)).toUpperCase()));

    Content content = file.getProtocolOutput(new Text(urlString), new CrawlDatum()).getContent();

    System.err.println("Content-Type: " + content.getContentType());
    System.err.println("Content-Length: " +
                       content.getMetadata().get(Response.CONTENT_LENGTH));
    System.err.println("Last-Modified: " +
                       content.getMetadata().get(Response.LAST_MODIFIED));
    if (dumpContent) {
      System.out.print(new String(content.getContent()));
    }

    file = null;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.maxContentLength = conf.getInt("file.content.limit", 64 * 1024);
  }

  public Configuration getConf() {
    return this.conf;
  }

  public RobotRules getRobotRules(Text url, CrawlDatum datum) {
    return EmptyRobotRules.RULES;
  }
}

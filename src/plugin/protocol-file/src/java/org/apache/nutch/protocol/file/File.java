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

import java.net.URL;
import java.util.Collection;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.EmptyRobotRules;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatusCodes;
import org.apache.nutch.protocol.ProtocolStatusUtils;
import org.apache.nutch.protocol.RobotRules;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.NutchConfiguration;

/************************************
 * File.java deals with file: scheme.
 * 
 * Configurable parameters are defined under "FILE properties" section in
 * ./conf/nutch-default.xml or similar.
 * 
 * @author John Xing
 ***********************************/
public class File implements Protocol {

  public static final Logger LOG = LoggerFactory.getLogger(File.class);

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.MODIFIED_TIME);
    FIELDS.add(WebPage.Field.HEADERS);
  }

  static final int MAX_REDIRECTS = 5;

  int maxContentLength;

  private Configuration conf;

  // constructor
  public File() {
  }

  /** Set the point at which content is truncated. */
  public void setMaxContentLength(int length) {
    maxContentLength = length;
  }

  public ProtocolOutput getProtocolOutput(String url, WebPage page) {
    String urlString = url.toString();
    try {
      URL u = new URL(urlString);

      int redirects = 0;

      while (true) {
        FileResponse response;
        response = new FileResponse(u, page, this, getConf()); // make
        // a
        // request

        int code = response.getCode();

        if (code == 200) { // got a good response
          return new ProtocolOutput(response.toContent()); // return
          // it

        } else if (code >= 300 && code < 400) { // handle redirect
          if (redirects == MAX_REDIRECTS)
            throw new FileException("Too many redirects: " + url);
          u = new URL(response.getHeader("Location"));
          redirects++;
          if (LOG.isTraceEnabled()) {
            LOG.trace("redirect to " + u);
          }

        } else { // convert to exception
          throw new FileError(code);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      ProtocolStatus ps = ProtocolStatusUtils.makeStatus(
          ProtocolStatusCodes.EXCEPTION, e.toString());
      return new ProtocolOutput(null, ps);
    }
  }

  @Override
  public RobotRules getRobotRules(String url, WebPage page) {
    return EmptyRobotRules.RULES;
  }

  @Override
  public Collection<Field> getFields() {
    return FIELDS;
  }

  /** For debugging. */
  public static void main(String[] args) throws Exception {
    int maxContentLength = Integer.MIN_VALUE;
    boolean dumpContent = false;
    String urlString = null;

    String usage = "Usage: File [-maxContentLength L] [-dumpContent] url";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-maxContentLength")) {
        maxContentLength = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-dumpContent")) {
        dumpContent = true;
      } else if (i != args.length - 1) {
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
    // LOG.setLevel(Level.parse((new String(logLevel)).toUpperCase()));

    Content content = file.getProtocolOutput(urlString, new WebPage())
        .getContent();

    System.out.println("Content-Type: " + content.getContentType());
    System.out.println("Content-Length: "
        + content.getMetadata().get(Response.CONTENT_LENGTH));
    System.out.println("Last-Modified: "
        + content.getMetadata().get(Response.LAST_MODIFIED));
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
}

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

import java.lang.invoke.MethodHandles;
import crawlercommons.robots.BaseRobotRules;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.*;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;

/**
 * This class is a protocol plugin used for file: scheme. It creates
 * {@link FileResponse} object and gets the content of the url from it.
 * Configurable parameters are {@code file.content.limit} and
 * {@code file.crawl.parent} in nutch-default.xml defined under
 * "file properties" section.
 */
public class File implements Protocol {

  protected static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.MODIFIED_TIME);
    FIELDS.add(WebPage.Field.HEADERS);
  }

  static final int MAX_REDIRECTS = 5;

  int maxContentLength;

  boolean crawlParents;

  /**
   * if true return a redirect for symbolic links and do not resolve the links
   * internally
   */
  boolean symlinksAsRedirects = true;

  private Configuration conf;

  // constructor
  public File() {
  }

  /**
   * Set the {@link Configuration} object
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.maxContentLength = conf.getInt("file.content.limit", 64 * 1024);
    this.crawlParents = conf.getBoolean("file.crawl.parent", true);
    this.symlinksAsRedirects = conf.getBoolean(
        "file.crawl.redirect_noncanonical", true);
  }

  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Set the point at which content is truncated.
   */
  public void setMaxContentLength(int maxContentLength) {
    this.maxContentLength = maxContentLength;
  }

  /**
   * Creates a {@link FileResponse} object corresponding to the url and return a
   * {@link ProtocolOutput} object as per the content received
   * 
   * @param url
   *          Text containing the url
   * @param page
   *          {@link WebPage} object relative to the URL
   * 
   * @return {@link ProtocolOutput} object for the content of the file indicated
   *         by url
   */
  public ProtocolOutput getProtocolOutput(String url, WebPage page) {
    String urlString = url.toString();
    try {
      URL u = new URL(urlString);

      int redirects = 0;

      while (true) {
        FileResponse response;
        response = new FileResponse(u, page, this, getConf()); // make a request
        int code = response.getCode();

        if (code == 200) { // got a good response
          return new ProtocolOutput(response.toContent()); // return it

        } else if (code == 304) { // got not modified
          return new ProtocolOutput(response.toContent(),
              ProtocolStatusUtils.STATUS_NOTMODIFIED);

        } else if (code == 401) { // access denied / no read permissions
          return new ProtocolOutput(response.toContent(),
              ProtocolStatusUtils.makeStatus(ProtocolStatusUtils.ACCESS_DENIED));

        } else if (code == 404) { // no such file
          return new ProtocolOutput(response.toContent(),
              ProtocolStatusUtils.STATUS_NOTFOUND);

        } else if (code >= 300 && code < 400) { // handle redirect
          u = new URL(response.getHeader("Location"));
          if (LOG.isTraceEnabled()) {
            LOG.trace("redirect to " + u);
          }
          if (symlinksAsRedirects) {
            return new ProtocolOutput(response.toContent(),
                ProtocolStatusUtils.makeStatus(ProtocolStatusUtils.MOVED, u));
          } else if (redirects == MAX_REDIRECTS) {
            LOG.trace("Too many redirects: {}", url);
            return new ProtocolOutput(response.toContent(),
                ProtocolStatusUtils.makeStatus(
                    ProtocolStatusUtils.REDIR_EXCEEDED, u));
          }
          redirects++;

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
  public Collection<Field> getFields() {
    return FIELDS;
  }

  /**
   * Quick way for running this class. Useful for debugging.
   */
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

    ProtocolOutput output = file.getProtocolOutput(urlString, WebPage
        .newBuilder().build());
    Content content = output.getContent();

    System.err.println("URL: " + content.getUrl());
    ProtocolStatus status = output.getStatus();
    String protocolMessage = ProtocolStatusUtils.getMessage(status);
    System.err.println("Status: "
        + ProtocolStatusUtils.getName(status.getCode())
        + (protocolMessage == null ? "" : ": " + protocolMessage));
    System.out.println("Content-Type: " + content.getContentType());
    System.out.println("Content-Length: "
        + content.getMetadata().get(Response.CONTENT_LENGTH));
    System.out.println("Last-Modified: "
        + content.getMetadata().get(Response.LAST_MODIFIED));
    String redirectLocation = content.getMetadata().get("Location");
    if (redirectLocation != null) {
      System.err.println("Location: " + redirectLocation);
    }

    if (dumpContent) {
      System.out.print(new String(content.getContent(), StandardCharsets.UTF_8));
    }

    file = null;
  }

  /**
   * No robots parsing is done for file protocol. So this returns a set of empty
   * rules which will allow every url.
   */
  public BaseRobotRules getRobotRules(String url, WebPage page) {
    return RobotRulesParser.EMPTY_RULES;
  }
}

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
package org.apache.nutch.protocol.foo;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.plugin.URLStreamHandlerFactory;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.protocol.RobotRulesParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crawlercommons.robots.BaseRobotRules;

public class Foo implements Protocol {
  protected static final Logger LOG = LoggerFactory.getLogger(Foo.class);

  private Configuration conf;

  @Override
  public Configuration getConf() {
    LOG.debug("getConf()");
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    LOG.debug("setConf(...)");
    this.conf = conf;
  }

  /**
   * This is a dummy implementation only. So what we will do is return this
   * structure:
   * 
   * <pre>
   * foo://example.com - will contain one directory and one file
   * foo://example.com/a - directory, will contain two files
   * foo://example.com/a/aa.txt - text file
   * foo://example.com/a/ab.txt - text file
   * foo://example.com/a.txt - text file
   * </pre>
   */
  @Override
  public ProtocolOutput getProtocolOutput(Text url, CrawlDatum datum) {
    LOG.debug("getProtocolOutput({}, {})", url, datum);

    try {
      String urlstr = String.valueOf(url);
      URL u = new URL(urlstr);
      URL base = new URL(u, ".");
      byte[] bytes = new byte[0];
      String contentType = "foo/something";
      ProtocolStatus status = ProtocolStatus.STATUS_GONE;

      switch (urlstr) {
      case "foo://example.com":
      case "foo://example.com/": {
        String time = HttpDateFormat.toString(System.currentTimeMillis());
        contentType = "text/html";
        StringBuffer sb = new StringBuffer();
        sb.append("<html><head>");
        sb.append("<title>Index of /</title></head>\n");
        sb.append("<body><h1>Index of /</h1><pre>\n");
        sb.append("<a href='a/" + "'>a/</a>\t"+ time + "\t-\n"); // add directory
        sb.append("<a href='a.txt'>a.txt</a>\t" + time + "\t" + 0 + "\n"); // add file
        sb.append("</pre></html></body>");
        bytes = sb.toString().getBytes();
        status = ProtocolStatus.STATUS_SUCCESS;
        break;
      }
      case "foo://example.com/a/": {
        String time = HttpDateFormat.toString(System.currentTimeMillis());
        contentType = "text/html";
        StringBuffer sb = new StringBuffer();
        sb.append("<html><head>");
        sb.append("<title>Index of /a/</title></head>\n");
        sb.append("<body><h1>Index of /a/</h1><pre>\n");
        sb.append("<a href='aa.txt'>aa.txt</a>\t" + time + "\t" + 0 + "\n"); // add file
        sb.append("<a href='ab.txt'>ab.txt</a>\t" + time + "\t" + 0 + "\n"); // add file
        sb.append("</pre></html></body>");
        bytes = sb.toString().getBytes();
        status = ProtocolStatus.STATUS_SUCCESS;
        break;
      }
      case "foo://example.com/a.txt":
      case "foo://example.com/a/aa.txt":
      case "foo://example.com/a/ab.txt": {
        contentType = "text/plain";
        bytes = "In publishing and graphic design, lorem ipsum is a filler text or greeking commonly used to demonstrate the textual elements of a graphic document or visual presentation. Replacing meaningful content with placeholder text allows designers to design the form of the content before the content itself has been produced.".getBytes();
        status = ProtocolStatus.STATUS_SUCCESS;
        break;
      }
      default:
        LOG.warn("Unknown url '{}'. This dummy implementation only supports 'foo://example.com'", url);
        // all our default values are set for URLs that do not exist.
        break;
      }

      Metadata metadata = new Metadata();
      Content content = new Content(String.valueOf(url), String.valueOf(base),
          bytes, contentType, metadata, getConf());

      return new ProtocolOutput(content, status);
    } catch (MalformedURLException mue) {
      LOG.error("Could not retrieve {}", url);
      LOG.error("", mue);
      // clain STATUS_GONE to tell nutch to never ever re-request this URL
      return new ProtocolOutput(null, ProtocolStatus.STATUS_GONE);
    }
  }

  @Override
  public BaseRobotRules getRobotRules(Text url, CrawlDatum datum,
      List<Content> robotsTxtContent) {
    LOG.debug(
        "getRobotRules({}, {}, {})", url, datum, robotsTxtContent);
    return RobotRulesParser.EMPTY_RULES;
  }
}

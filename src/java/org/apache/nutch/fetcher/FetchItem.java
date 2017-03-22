/*
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
package org.apache.nutch.fetcher;

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;

import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.util.URLUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * This class describes the item to be fetched.
 */
public class FetchItem {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  int outlinkDepth = 0;
  String queueID;
  Text url;
  URL u;
  CrawlDatum datum;

  public FetchItem(Text url, URL u, CrawlDatum datum, String queueID) {
    this(url, u, datum, queueID, 0);
  }

  public FetchItem(Text url, URL u, CrawlDatum datum, String queueID,
      int outlinkDepth) {
    this.url = url;
    this.u = u;
    this.datum = datum;
    this.queueID = queueID;
    this.outlinkDepth = outlinkDepth;
  }

  /**
   * Create an item. Queue id will be created based on <code>queueMode</code>
   * argument, either as a protocol + hostname pair, protocol + IP address
   * pair or protocol+domain pair.
   */
  public static FetchItem create(Text url, CrawlDatum datum, String queueMode) {
    return create(url, datum, queueMode, 0);
  }

  public static FetchItem create(Text url, CrawlDatum datum,
      String queueMode, int outlinkDepth) {
    String queueID;
    URL u = null;
    try {
      u = new URL(url.toString());
    } catch (Exception e) {
      LOG.warn("Cannot parse url: " + url, e);
      return null;
    }
    final String proto = u.getProtocol().toLowerCase();
    String key;
    if (FetchItemQueues.QUEUE_MODE_IP.equalsIgnoreCase(queueMode)) {
      try {
        final InetAddress addr = InetAddress.getByName(u.getHost());
        key = addr.getHostAddress();
      } catch (final UnknownHostException e) {
        // unable to resolve it, so don't fall back to host name
        LOG.warn("Unable to resolve: " + u.getHost() + ", skipping.");
        return null;
      }
    } else if (FetchItemQueues.QUEUE_MODE_DOMAIN.equalsIgnoreCase(queueMode)) {
      key = URLUtil.getDomainName(u);
      if (key == null) {
        LOG.warn("Unknown domain for url: " + url
            + ", using URL string as key");
        key = u.toExternalForm();
      }
    } else {
      key = u.getHost();
      if (key == null) {
        LOG.warn("Unknown host for url: " + url + ", using URL string as key");
        key = u.toExternalForm();
      }
    }
    queueID = proto + "://" + key.toLowerCase();
    return new FetchItem(url, u, datum, queueID, outlinkDepth);
  }

  public CrawlDatum getDatum() {
    return datum;
  }

  public String getQueueID() {
    return queueID;
  }

  public Text getUrl() {
    return url;
  }

  public URL getURL2() {
    return u;
  }
}

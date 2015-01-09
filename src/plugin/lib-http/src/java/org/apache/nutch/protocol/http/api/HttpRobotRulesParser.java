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

package org.apache.nutch.protocol.http.api;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.RobotRulesParser;
import org.apache.nutch.storage.WebPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

/**
 * This class is used for parsing robots for urls belonging to HTTP protocol. It
 * extends the generic {@link RobotRulesParser} class and contains Http protocol
 * specific implementation for obtaining the robots file.
 */
public class HttpRobotRulesParser extends RobotRulesParser {

  public static final Logger LOG = LoggerFactory
      .getLogger(HttpRobotRulesParser.class);
  protected boolean allowForbidden = false;

  HttpRobotRulesParser() {
  }

  public HttpRobotRulesParser(Configuration conf) {
    super(conf);
    allowForbidden = conf.getBoolean("http.robots.403.allow", false);
  }

  /** Compose unique key to store and access robot rules in cache for given URL */
  protected static String getCacheKey(URL url) {
    String protocol = url.getProtocol().toLowerCase(); // normalize to lower
                                                       // case
    String host = url.getHost().toLowerCase(); // normalize to lower case
    int port = url.getPort();
    if (port == -1) {
      port = url.getDefaultPort();
    }
    /*
     * Robot rules apply only to host, protocol, and port where robots.txt is
     * hosted (cf. NUTCH-1752). Consequently
     */
    String cacheKey = protocol + ":" + host + ":" + port;
    return cacheKey;
  }

  /**
   * Get the rules from robots.txt which applies for the given {@code url}.
   * Robot rules are cached for a unique combination of host, protocol, and
   * port. If no rules are found in the cache, a HTTP request is send to fetch
   * {{protocol://host:port/robots.txt}}. The robots.txt is then parsed and the
   * rules are cached to avoid re-fetching and re-parsing it again.
   * 
   * @param http
   *          The {@link Protocol} object
   * @param url
   *          URL robots.txt applies to
   * 
   * @return {@link BaseRobotRules} holding the rules from robots.txt
   */
  public BaseRobotRules getRobotRulesSet(Protocol http, URL url) {

    String cacheKey = getCacheKey(url);
    BaseRobotRules robotRules = (SimpleRobotRules) CACHE.get(cacheKey);

    boolean cacheRule = true;

    if (robotRules == null) { // cache miss
      URL redir = null;
      if (LOG.isTraceEnabled()) {
        LOG.trace("cache miss " + url);
      }
      try {
        Response response = ((HttpBase) http).getResponse(new URL(url,
            "/robots.txt"), WebPage.newBuilder().build(), true);
        // try one level of redirection ?
        if (response.getCode() == 301 || response.getCode() == 302) {
          String redirection = response.getHeader("Location");
          if (redirection == null) {
            // some versions of MS IIS are known to mangle this header
            redirection = response.getHeader("location");
          }
          if (redirection != null) {
            if (!redirection.startsWith("http")) {
              // RFC says it should be absolute, but apparently it isn't
              redir = new URL(url, redirection);
            } else {
              redir = new URL(redirection);
            }

            response = ((HttpBase) http).getResponse(redir, WebPage
                .newBuilder().build(), true);
          }
        }

        if (response.getCode() == 200) // found rules: parse them
          robotRules = parseRules(url.toString(), response.getContent(),
              response.getHeader("Content-Type"), agentNames);

        else if ((response.getCode() == 403) && (!allowForbidden))
          robotRules = FORBID_ALL_RULES; // use forbid all
        else if (response.getCode() >= 500) {
          cacheRule = false;
          robotRules = EMPTY_RULES;
        } else
          robotRules = EMPTY_RULES; // use default rules
      } catch (Throwable t) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Couldn't get robots.txt for " + url + ": " + t.toString());
        }
        cacheRule = false;
        robotRules = EMPTY_RULES;
      }

      if (cacheRule) {
        CACHE.put(cacheKey, robotRules); // cache rules for host
        if (redir != null && !redir.getHost().equalsIgnoreCase(url.getHost())) {
          // cache also for the redirected host
          CACHE.put(getCacheKey(redir), robotRules);
        }
      }
    }
    return robotRules;
  }
}

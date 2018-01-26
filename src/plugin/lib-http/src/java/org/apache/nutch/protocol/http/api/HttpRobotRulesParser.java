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

import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.RobotRulesParser;

import crawlercommons.robots.BaseRobotRules;

/**
 * This class is used for parsing robots for urls belonging to HTTP protocol. It
 * extends the generic {@link RobotRulesParser} class and contains Http protocol
 * specific implementation for obtaining the robots file.
 */
public class HttpRobotRulesParser extends RobotRulesParser {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  protected boolean allowForbidden = false;

  HttpRobotRulesParser() {
  }

  public HttpRobotRulesParser(Configuration conf) {
    setConf(conf);
  }

  public void setConf(Configuration conf) {
    super.setConf(conf);
    allowForbidden = conf.getBoolean("http.robots.403.allow", true);
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
   *          URL
   * @param robotsTxtContent
   *          container to store responses when fetching the robots.txt file for
   *          debugging or archival purposes. Instead of a robots.txt file, it
   *          may include redirects or an error page (404, etc.). Response
   *          {@link Content} is appended to the passed list. If null is passed
   *          nothing is stored.
   *
   * @return robotRules A {@link BaseRobotRules} object for the rules
   */
  @Override
  public BaseRobotRules getRobotRulesSet(Protocol http, URL url,
      List<Content> robotsTxtContent) {

    if (LOG.isTraceEnabled() && isWhiteListed(url)) {
      LOG.trace("Ignoring robots.txt (host is whitelisted) for URL: {}", url);
    }

    String cacheKey = getCacheKey(url);
    BaseRobotRules robotRules = CACHE.get(cacheKey);

    if (robotRules != null) {
      return robotRules; // cached rule
    } else if (LOG.isTraceEnabled()) {
      LOG.trace("cache miss " + url);
    }

    boolean cacheRule = true;
    URL redir = null;

    if (isWhiteListed(url)) {
      // check in advance whether a host is whitelisted
      // (we do not need to fetch robots.txt)
      robotRules = EMPTY_RULES;
      LOG.info("Whitelisted host found for: {}", url);
      LOG.info("Ignoring robots.txt for all URLs from whitelisted host: {}",
          url.getHost());

    } else {
      try {
        URL robotsUrl = new URL(url, "/robots.txt");
        Response response = ((HttpBase) http).getResponse(robotsUrl,
            new CrawlDatum(), true);
        if (robotsTxtContent != null) {
          addRobotsContent(robotsTxtContent, robotsUrl, response);
        }
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

            response = ((HttpBase) http).getResponse(redir, new CrawlDatum(),
                true);
            if (robotsTxtContent != null) {
              addRobotsContent(robotsTxtContent, redir, response);
            }
          }
        }

        if (response.getCode() == 200) // found rules: parse them
          robotRules = parseRules(url.toString(), response.getContent(),
              response.getHeader("Content-Type"), agentNames);

        else if ((response.getCode() == 403) && (!allowForbidden))
          robotRules = FORBID_ALL_RULES; // use forbid all
        else if (response.getCode() >= 500) {
          cacheRule = false; // try again later to fetch robots.txt
          robotRules = EMPTY_RULES;
        } else
          robotRules = EMPTY_RULES; // use default rules
      } catch (Throwable t) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Couldn't get robots.txt for " + url + ": " + t.toString());
        }
        cacheRule = false; // try again later to fetch robots.txt
        robotRules = EMPTY_RULES;
      }
    }

    if (cacheRule) {
      CACHE.put(cacheKey, robotRules); // cache rules for host
      if (redir != null && !redir.getHost().equalsIgnoreCase(url.getHost())) {
        // cache also for the redirected host
        CACHE.put(getCacheKey(redir), robotRules);
      }
    }

    return robotRules;
  }

  /**
   * Append {@link Content} of robots.txt to {@literal robotsTxtContent}
   * 
   * @param robotsTxtContent
   *          container to store robots.txt response content
   * @param robotsUrl
   *          robots.txt URL
   * @param robotsResponse
   *          response object to be stored
   */
  protected void addRobotsContent(List<Content> robotsTxtContent,
      URL robotsUrl, Response robotsResponse) {
    byte[] robotsBytes = robotsResponse.getContent();
    if (robotsBytes == null)
      robotsBytes = new byte[0];
    Content content = new Content(robotsUrl.toString(),
        robotsUrl.toString(), robotsBytes,
        robotsResponse.getHeader("Content-Type"), robotsResponse.getHeaders(),
        getConf());
    robotsTxtContent.add(content);
  }

}

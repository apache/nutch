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
package org.apache.nutch.protocol.http.api;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;
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
  protected boolean deferVisits503 = false;

  HttpRobotRulesParser() {
  }

  public HttpRobotRulesParser(Configuration conf) {
    setConf(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    allowForbidden = conf.getBoolean("http.robots.403.allow", true);
    deferVisits503 = conf.getBoolean("http.robots.503.defer.visits", true);
  }

  /**
   * Compose unique key to store and access robot rules in cache for given URL
   * @param url to generate a unique key for
   * @return the cached unique key
   */
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
   * <p>Following
   * <a href="https://www.rfc-editor.org/rfc/rfc9309.html#section-2.3.1.2">RFC
   * 9309, section 2.3.1.2. Redirects</a>, up to five consecutive HTTP redirects
   * are followed when fetching the robots.txt file. The max. number of
   * redirects followed is configurable by the property
   * <code>http.robots.redirect.max</code>.</p>
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

    if (LOG.isTraceEnabled() && isAllowListed(url)) {
      LOG.trace("Ignoring robots.txt (host is allowlisted) for URL: {}", url);
    }

    String cacheKey = getCacheKey(url);
    BaseRobotRules robotRules = CACHE.get(cacheKey);

    if (robotRules != null) {
      return robotRules; // cached rule
    } else if (LOG.isTraceEnabled()) {
      LOG.trace("Robots.txt cache miss {}", url);
    }

    boolean cacheRule = true;
    Set<String> redirectCacheKeys = new HashSet<>();

    if (isAllowListed(url)) {
      // check in advance whether a host is allowlisted
      // (we do not need to fetch robots.txt)
      robotRules = EMPTY_RULES;
      LOG.info("Allowlisted host found for: {}", url);
      LOG.info("Ignoring robots.txt for all URLs from allowlisted host: {}",
          url.getHost());

    } else {
      URL robotsUrl = null, robotsUrlRedir = null;
      try {
        robotsUrl = new URL(url, "/robots.txt");

        /*
         * Redirect counter - following redirects up to the configured maximum
         * ("five consecutive redirects" as per RFC 9309).
         */
        int numRedirects = 0;
        /*
         * The base URL to resolve relative redirect locations is the to the
         * default URL path ("/robots.txt") and updated when redirects were
         * followed.
         */
        robotsUrlRedir = robotsUrl;

        Response response = ((HttpBase) http).getResponse(robotsUrl,
            new CrawlDatum(), true);
        int code = response.getCode();
        if (robotsTxtContent != null) {
          addRobotsContent(robotsTxtContent, robotsUrl, response);
        }

        while (isRedirect(code) && numRedirects < maxNumRedirects) {
          numRedirects++;

          String redirectionLocation = response.getHeader("Location");
          if (StringUtils.isNotBlank(redirectionLocation)) {
            LOG.debug("Following robots.txt redirect: {} -> {}", robotsUrlRedir,
                redirectionLocation);
            try {
              robotsUrlRedir = new URL(robotsUrlRedir, redirectionLocation);
            } catch (MalformedURLException e) {
              LOG.info(
                  "Failed to resolve redirect location for robots.txt: {} -> {} ({})",
                  robotsUrlRedir, redirectionLocation, e.getMessage());
              break;
            }
            response = ((HttpBase) http).getResponse(robotsUrlRedir,
                new CrawlDatum(), true);
            code = response.getCode();
            if (robotsTxtContent != null) {
              addRobotsContent(robotsTxtContent, robotsUrlRedir, response);
            }
          } else {
            LOG.info(
                "No HTTP redirect Location header for robots.txt: {} (status code: {})",
                robotsUrlRedir, code);
            break;
          }

          if ("/robots.txt".equals(robotsUrlRedir.getFile())) {
            /*
             * If a redirect points to a path /robots.txt on a different host
             * (or a different authority scheme://host:port/, in general), we
             * can lookup the cache for cached rules from the target host.
             */
            String redirectCacheKey = getCacheKey(robotsUrlRedir);
            robotRules = CACHE.get(redirectCacheKey);
            LOG.debug(
                "Found cached robots.txt rules for {} (redirected to {}) under target key {}",
                url, robotsUrlRedir, redirectCacheKey);
            if (robotRules != null) {
              /* If found, cache and return the rules for the source host. */
              CACHE.put(cacheKey, robotRules);
              return robotRules;
            } else {
              /*
               * Remember the target host/authority, we can cache the rules,
               * too.
               */
              redirectCacheKeys.add(redirectCacheKey);
            }
          }

          if (numRedirects == maxNumRedirects && isRedirect(code)) {
            LOG.info(
                "Reached maximum number of robots.txt redirects for {} (assuming no robots.txt, allow all)",
                url);
          }
        }

        LOG.debug("Fetched robots.txt for {} with status code {}", url, code);
        if (code == 200) // found rules: parse them
          robotRules = parseRules(url.toString(), response.getContent(),
              response.getHeader("Content-Type"), agentNames);

        else if ((code == 403) && (!allowForbidden))
          robotRules = FORBID_ALL_RULES; // use forbid all

        else if (code >= 500) {
          cacheRule = false; // try again later to fetch robots.txt
          if (deferVisits503) {
            // signal fetcher to suspend crawling for this host
            robotRules = DEFER_VISIT_RULES;
          } else {
            robotRules = EMPTY_RULES;
          }
        } else {
          robotRules = EMPTY_RULES; // use default rules
        }
      } catch (Throwable t) {
        if (robotsUrl == null || robotsUrlRedir == null) {
          LOG.info("Couldn't get robots.txt for {}", url, t);
        } else if (robotsUrl.equals(robotsUrlRedir)) {
          LOG.info("Couldn't get robots.txt for {} ({}): {}", url, robotsUrl,
              t);
        } else {
          LOG.info(
              "Couldn't get redirected robots.txt for {} (redirected to {}): {}",
              url, robotsUrlRedir, t);
        }
        cacheRule = false; // try again later to fetch robots.txt
        robotRules = EMPTY_RULES;
      }
    }

    if (cacheRule) {
      CACHE.put(cacheKey, robotRules); // cache rules for host
      for (String redirectCacheKey : redirectCacheKeys) {
        /*
         * and also for redirect target hosts where URL path and query were
         * found to be "/robots.txt"
         */
        CACHE.put(redirectCacheKey, robotRules);
      }
    }

    return robotRules;
  }

  /**
   * @param code
   *          HTTP response status code
   * @return whether the status code signals a redirect to a different location
   */
  private boolean isRedirect(int code) {
    return (code == 301 || code == 302 || code == 303 || code == 307 || code == 308);
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

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

import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatusCodes;
import org.apache.nutch.protocol.RobotRulesParser;
import org.apache.nutch.storage.WebPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;

/**
 * This class is used for parsing robots for urls belonging to FTP protocol.
 * It extends the generic {@link RobotRulesParser} class and contains 
 * Ftp protocol specific implementation for obtaining the robots file.
 */
public class FtpRobotRulesParser extends RobotRulesParser {

  private static final String CONTENT_TYPE = "text/plain";
  public static final Logger LOG = LoggerFactory.getLogger(FtpRobotRulesParser.class);

  FtpRobotRulesParser() { }

  public FtpRobotRulesParser(Configuration conf) {
    super(conf);
  }

  /**
   * The hosts for which the caching of robots rules is yet to be done,
   * it sends a Ftp request to the host corresponding to the {@link URL} 
   * passed, gets robots file, parses the rules and caches the rules object
   * to avoid re-work in future.
   * 
   *  @param ftp The {@link Protocol} object
   *  @param url URL 
   *  
   *  @return robotRules A {@link BaseRobotRules} object for the rules
   */
  public BaseRobotRules getRobotRulesSet(Protocol ftp, URL url) {

    String protocol = url.getProtocol().toLowerCase();  // normalize to lower case
    String host = url.getHost().toLowerCase();          // normalize to lower case

    BaseRobotRules robotRules = (SimpleRobotRules) CACHE.get(protocol + ":" + host);

    boolean cacheRule = true;

    if (robotRules == null) {                     // cache miss

      if (LOG.isTraceEnabled())
        LOG.trace("cache miss " + url);

      try {
        String robotsUrl = new URL(url, "/robots.txt").toString();        
        ProtocolOutput output = ((Ftp)ftp).getProtocolOutput(robotsUrl, new WebPage());
        int statusCode = output.getStatus().getCode();

        if (statusCode == ProtocolStatusCodes.SUCCESS) {
          robotRules =  parseRules(url.toString(), output.getContent().getContent(), 
                                  CONTENT_TYPE, agentNames);
        } else {                                       
          robotRules = EMPTY_RULES;                 // use default rules
        }
      } catch (Throwable t) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Couldn't get robots.txt for " + url + ": " + t.toString());
        }
        cacheRule = false;
        robotRules = EMPTY_RULES;
      }

      if (cacheRule)
        CACHE.put(protocol + ":" + host, robotRules);  // cache rules for host
    }
    return robotRules;
  }
}

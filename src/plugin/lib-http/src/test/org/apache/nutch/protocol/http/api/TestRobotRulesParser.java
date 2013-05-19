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

import org.junit.Before;
import org.junit.Test;

import crawlercommons.robots.BaseRobotRules;

import static org.junit.Assert.*;

/**
 * JUnit test case which tests
 * 1. that robots filtering is performed correctly as per the agent name
 * 2. that crawl delay is extracted correctly from the robots file
 *
 */
public class TestRobotRulesParser {

  private static final String CONTENT_TYPE = "text/plain";
  private static final String SINGLE_AGENT = "Agent1";
  private static final String MULTIPLE_AGENTS = "Agent2, Agent1";
  private static final String UNKNOWN_AGENT = "AgentABC";
  private static final String CR = "\r";
  
  private static final String ROBOTS_STRING = 
      "User-Agent: Agent1 #foo" + CR 
      + "Disallow: /a" + CR 
      + "Disallow: /b/a" + CR 
      + "#Disallow: /c" + CR 
      + "Crawl-delay: 10" + CR  // set crawl delay for Agent1 as 10 sec
      + "" + CR 
      + "" + CR 
      + "User-Agent: Agent2" + CR 
      + "Disallow: /a/bloh" + CR 
      + "Disallow: /c" + CR
      + "Disallow: /foo" + CR
      + "Crawl-delay: 20" + CR
      + "" + CR 
      + "User-Agent: *" + CR 
      + "Disallow: /foo/bar/" + CR;   // no crawl delay for other agents
  
  private static final String[] TEST_PATHS = new String[] {
    "http://example.com/a",
    "http://example.com/a/bloh/foo.html",
    "http://example.com/b",
    "http://example.com/c",
    "http://example.com/b/a/index.html",
    "http://example.com/foo/bar/baz.html"
  };

  private static final boolean[] RESULTS = new boolean[] {
    false,  //  /a
    false,  //  /a/bloh/foo.html
    true,   //  /b
    true,   //  /c
    false,  //  /b/a/index.html
    true    //  /foo/bar/baz.html
  };

  private HttpRobotRulesParser parser;
  private BaseRobotRules rules;

  @Before
  public void setUp() {
    parser = new HttpRobotRulesParser();
  }

  /**
  * Test that the robots rules are interpreted correctly by the robots rules parser. 
  */
  @Test
  public void testRobotsAgent() {
    rules = parser.parseRules("testRobotsAgent", ROBOTS_STRING.getBytes(), CONTENT_TYPE, SINGLE_AGENT);

    for(int counter = 0; counter < TEST_PATHS.length; counter++) {
      assertTrue("testing on agent (" + SINGLE_AGENT + "), and " 
              + "path " + TEST_PATHS[counter] 
              + " got " + rules.isAllowed(TEST_PATHS[counter]),
              rules.isAllowed(TEST_PATHS[counter]) == RESULTS[counter]);
    }

    rules = parser.parseRules("testRobotsAgent", ROBOTS_STRING.getBytes(), CONTENT_TYPE, MULTIPLE_AGENTS);

    for(int counter = 0; counter < TEST_PATHS.length; counter++) {
      assertTrue("testing on agents (" + MULTIPLE_AGENTS + "), and " 
              + "path " + TEST_PATHS[counter] 
              + " got " + rules.isAllowed(TEST_PATHS[counter]),
              rules.isAllowed(TEST_PATHS[counter]) == RESULTS[counter]);
    }
  }

  /**
  * Test that the crawl delay is extracted from the robots file for respective agent. 
  * If its not specified for a given agent, default value must be returned.
  */
  @Test
  public void testCrawlDelay() {
    // for SINGLE_AGENT, the crawl delay of 10 sec ie. 10000 msec must be returned by the parser
    rules = parser.parseRules("testCrawlDelay", ROBOTS_STRING.getBytes(), CONTENT_TYPE, SINGLE_AGENT);
    assertTrue("testing crawl delay for agent "+ SINGLE_AGENT +" : ", (rules.getCrawlDelay() == 10000));
    
    // for UNKNOWN_AGENT, the default crawl delay must be returned.
    rules = parser.parseRules("testCrawlDelay", ROBOTS_STRING.getBytes(), CONTENT_TYPE, UNKNOWN_AGENT);
    assertTrue("testing crawl delay for agent "+ UNKNOWN_AGENT +" : ", (rules.getCrawlDelay() == Long.MIN_VALUE));
  }
}

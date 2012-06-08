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

import org.apache.nutch.protocol.http.api.RobotRulesParser.RobotRuleSet;

import junit.framework.TestCase;

public class TestRobotRulesParser extends TestCase {
  private static final String LF= "\n";
  private static final String CR= "\r";
  private static final String CRLF= "\r\n";
  
  private static final boolean[] ACCEPT_ALL = {
    true,   // "/a",	      
    true,   // "/a/",	      
    true,   // "/a/bloh/foo.html"
    true,   // "/b",	      
    true,   // "/b/a",	      
    true,   // "/b/a/index.html",
    true,   // "/b/b/foo.html",  
    true,   // "/c",	      
    true,   // "/c/a",	      
    true,   // "/c/a/index.html",
    true,   // "/c/b/foo.html",  
    true,   // "/d",	      
    true,   // "/d/a",	      
    true,   // "/e/a/index.html",
    true,   // "/e/d",	      
    true,   // "/e/d/foo.html",  
    true,   // "/e/doh.html",    
    true,   // "/f/index.html",  
    true,   // "/foo/bar.html",  
    true,   // "/f/",
  };
  
  private static final String[] ROBOTS_STRINGS= new String[] {
    "User-Agent: Agent1 #foo" + CR 
    + "Disallow: /a" + CR 
    + "Disallow: /b/a" + CR 
    + "#Disallow: /c" + CR 
    + "" + CR 
    + "" + CR 
    + "User-Agent: Agent2 Agent3#foo" + CR 
    + "User-Agent: Agent4" + CR 
    + "Disallow: /d" + CR 
    + "Disallow: /e/d/" + CR
    + "" + CR 
    + "User-Agent: *" + CR 
    + "Disallow: /foo/bar/" + CR,
    null  // Used to test EMPTY_RULES
  };

  private static final String[] AGENT_STRINGS= new String[] {
    "Agent1",
    "Agent2",
    "Agent3",
    "Agent4",
    "Agent5",
  };

  private static final boolean[][] NOT_IN_ROBOTS_STRING= new boolean[][] {
    { 
      false, 
      false,
      false,
      false,
      true,
    },
    { 
      false, 
      false,
      false,
      false,
      true,
    }    
  };

  private static final String[] TEST_PATHS= new String[] {
    "/a",
    "/a/",
    "/a/bloh/foo.html",
    "/b",
    "/b/a",
    "/b/a/index.html",
    "/b/b/foo.html",
    "/c",
    "/c/a",
    "/c/a/index.html",
    "/c/b/foo.html",
    "/d",
    "/d/a",
    "/e/a/index.html",
    "/e/d",
    "/e/d/foo.html",
    "/e/doh.html",
    "/f/index.html",
    "/foo/bar/baz.html",  
    "/f/",
  };

  private static final boolean[][][] ALLOWED= new boolean[][][] {
    { // ROBOTS_STRINGS[0]
      { // Agent1
	false,  // "/a",	      
	false,  // "/a/",	      
	false,  // "/a/bloh/foo.html"
	true,   // "/b",	      
	false,  // "/b/a",	      
	false,  // "/b/a/index.html",
	true,   // "/b/b/foo.html",  
	true,   // "/c",	      
	true,   // "/c/a",	      
	true,   // "/c/a/index.html",
	true,   // "/c/b/foo.html",  
	true,   // "/d",	      
	true,   // "/d/a",	      
	true,   // "/e/a/index.html",
	true,   // "/e/d",	      
	true,   // "/e/d/foo.html",  
	true,   // "/e/doh.html",    
	true,   // "/f/index.html",  
	true,   // "/foo/bar.html",  
	true,   // "/f/",  
      }, 
      { // Agent2
	true,   // "/a",	      
	true,   // "/a/",	      
	true,   // "/a/bloh/foo.html"
	true,   // "/b",	      
	true,   // "/b/a",	      
	true,   // "/b/a/index.html",
	true,   // "/b/b/foo.html",  
	true,   // "/c",	      
	true,   // "/c/a",	      
	true,   // "/c/a/index.html",
	true,   // "/c/b/foo.html",  
	false,  // "/d",	      
	false,  // "/d/a",	      
	true,   // "/e/a/index.html",
	true,   // "/e/d",	      
	false,  // "/e/d/foo.html",  
	true,   // "/e/doh.html",    
	true,   // "/f/index.html",  
	true,   // "/foo/bar.html",  
	true,   // "/f/",  
      },
      { // Agent3
	true,   // "/a",	      
	true,   // "/a/",	      
	true,   // "/a/bloh/foo.html"
	true,   // "/b",	      
	true,   // "/b/a",	      
	true,   // "/b/a/index.html",
	true,   // "/b/b/foo.html",  
	true,   // "/c",	      
	true,   // "/c/a",	      
	true,   // "/c/a/index.html",
	true,   // "/c/b/foo.html",  
	false,  // "/d",	      
	false,  // "/d/a",	      
	true,   // "/e/a/index.html",
	true,   // "/e/d",	      
	false,  // "/e/d/foo.html",  
	true,   // "/e/doh.html",    
	true,   // "/f/index.html",  
	true,   // "/foo/bar.html",  
	true,   // "/f/",  
      },
      { // Agent4
	true,   // "/a",	      
	true,   // "/a/",	      
	true,   // "/a/bloh/foo.html"
	true,   // "/b",	      
	true,   // "/b/a",	      
	true,   // "/b/a/index.html",
	true,   // "/b/b/foo.html",  
	true,   // "/c",	      
	true,   // "/c/a",	      
	true,   // "/c/a/index.html",
	true,   // "/c/b/foo.html",  
	false,  // "/d",	      
	false,  // "/d/a",	      
	true,   // "/e/a/index.html",
	true,   // "/e/d",	      
	false,  // "/e/d/foo.html",  
	true,   // "/e/doh.html",    
	true,   // "/f/index.html",  
	true,   // "/foo/bar.html",  
	true,   // "/f/",  
      },
      { // Agent5/"*"
	true,   // "/a",	      
	true,   // "/a/",	      
	true,   // "/a/bloh/foo.html"
	true,   // "/b",	      
	true,   // "/b/a",	      
	true,   // "/b/a/index.html",
	true,   // "/b/b/foo.html",  
	true,   // "/c",	      
	true,   // "/c/a",	      
	true,   // "/c/a/index.html",
	true,   // "/c/b/foo.html",  
	true,   // "/d",	      
	true,   // "/d/a",	      
	true,   // "/e/a/index.html",
	true,   // "/e/d",	      
	true,   // "/e/d/foo.html",  
	true,   // "/e/doh.html",    
	true,   // "/f/index.html",  
	false,  // "/foo/bar.html",  
	true,   // "/f/",  
      }
    },
    { // ROBOTS_STRINGS[1]
      ACCEPT_ALL, // Agent 1
      ACCEPT_ALL, // Agent 2
      ACCEPT_ALL, // Agent 3
      ACCEPT_ALL, // Agent 4
      ACCEPT_ALL, // Agent 5
    }
  };
 
  public TestRobotRulesParser(String name) {
    super(name);
  }

  public void testRobotsOneAgent() {
    for (int i= 0; i < ROBOTS_STRINGS.length; i++) {
      for (int j= 0; j < AGENT_STRINGS.length; j++) {
	testRobots(i, new String[] { AGENT_STRINGS[j] },
		   TEST_PATHS, ALLOWED[i][j]);
      }
    }
  }

  public void testRobotsTwoAgents() {
    for (int i= 0; i < ROBOTS_STRINGS.length; i++) {
      for (int j= 0; j < AGENT_STRINGS.length; j++) {
	for (int k= 0; k < AGENT_STRINGS.length; k++) {
	  int key= j;
	  if (NOT_IN_ROBOTS_STRING[i][j])
	    key= k;
	  testRobots(i, new String[] { AGENT_STRINGS[j], AGENT_STRINGS[k] },
		     TEST_PATHS, ALLOWED[i][key]);
	}
      }
    }
  }
  
  public void testCrawlDelay() {
    RobotRulesParser p = new RobotRulesParser(new String[] { "nutchbot" });
    String delayRule1 = "User-agent: nutchbot" + CR +
                        "Crawl-delay: 10" + CR +
                        "User-agent: foobot" + CR +
                        "Crawl-delay: 20" + CR +
                        "User-agent: *" + CR + 
                        "Disallow:/baz" + CR;
    String delayRule2 = "User-agent: foobot" + CR +
                        "Crawl-delay: 20" + CR +
                        "User-agent: *" + CR + 
                        "Disallow:/baz" + CR;
    RobotRuleSet rules = p.parseRules(delayRule1.getBytes());
    long crawlDelay = rules.getCrawlDelay();
    assertTrue("testing crawl delay for agent nutchbot - rule 1", (crawlDelay == 10000));
    rules = p.parseRules(delayRule2.getBytes());
    crawlDelay = rules.getCrawlDelay();
    assertTrue("testing crawl delay for agent nutchbot - rule 2", (crawlDelay == -1));
  }

  // helper

  public void testRobots(int robotsString, String[] agents, String[] paths, 
			 boolean[] allowed) {
    String agentsString= agents[0];
    for (int i= 1; i < agents.length; i++)
      agentsString= agentsString + "," + agents[i];
    RobotRulesParser p= new RobotRulesParser(agents);
    RobotRuleSet rules= p.parseRules(ROBOTS_STRINGS[robotsString] != null
                                     ? ROBOTS_STRINGS[robotsString].getBytes()
                                     : null);
    for (int i= 0; i < paths.length; i++) {
      assertTrue("testing robots file "+robotsString+", on agents ("
		 + agentsString + "), and path " + TEST_PATHS[i] + "; got " 
		 + rules.isAllowed(TEST_PATHS[i]) + ", rules are: " + LF
				   + rules,
		 rules.isAllowed(TEST_PATHS[i]) == allowed[i]);
    }
  }


  
}

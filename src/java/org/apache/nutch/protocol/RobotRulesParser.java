/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.nutch.protocol;

// JDK imports
import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.StringTokenizer;

// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Nutch imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.Text;

import com.google.common.io.Files;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRules.RobotRulesMode;
import crawlercommons.robots.SimpleRobotRulesParser;

/**
 * This class uses crawler-commons for handling the parsing of {@code robots.txt} files.
 * It emits SimpleRobotRules objects, which describe the download permissions
 * as described in SimpleRobotRulesParser.
 */
public abstract class RobotRulesParser implements Configurable {

  public static final Logger LOG = LoggerFactory.getLogger(RobotRulesParser.class);

  protected static final Hashtable<String, BaseRobotRules> CACHE = new Hashtable<String, BaseRobotRules> ();

  /**
   *  A {@link BaseRobotRules} object appropriate for use
   *  when the {@code robots.txt} file is empty or missing;
   *  all requests are allowed.
   */
  public static final BaseRobotRules EMPTY_RULES = new SimpleRobotRules(RobotRulesMode.ALLOW_ALL);

  /**
   *  A {@link BaseRobotRules} object appropriate for use when the 
   *  {@code robots.txt} file is not fetched due to a {@code 403/Forbidden}
   *  response; all requests are disallowed. 
   */
  public static BaseRobotRules FORBID_ALL_RULES = new SimpleRobotRules(RobotRulesMode.ALLOW_NONE);

  private static SimpleRobotRulesParser robotParser = new SimpleRobotRulesParser();
  private Configuration conf;
  protected String agentNames;

  public RobotRulesParser() { }

  public RobotRulesParser(Configuration conf) {
    setConf(conf);
  }

  /**
   * Set the {@link Configuration} object
   */
  public void setConf(Configuration conf) {
    this.conf = conf;

    // Grab the agent names we advertise to robots files.
    String agentName = conf.get("http.agent.name");
    if (null == agentName) {
      throw new RuntimeException("Agent name not configured!");
    }

    String agentNames = conf.get("http.robots.agents");
    StringTokenizer tok = new StringTokenizer(agentNames, ",");
    ArrayList<String> agents = new ArrayList<String>();
    while (tok.hasMoreTokens()) {
      agents.add(tok.nextToken().trim());
    }

    /**
     * If there are no agents for robots-parsing, use the
     * default agent-string. If both are present, our agent-string
     * should be the first one we advertise to robots-parsing.
     */
    if (agents.size() == 0) {
      if (LOG.isErrorEnabled()) {
        LOG.error("No agents listed in 'http.robots.agents' property!");
      }
    } else { 
      StringBuffer combinedAgentsString = new StringBuffer(agentName);
      int index = 0;

      if ((agents.get(0)).equalsIgnoreCase(agentName))
        index++;
      else if (LOG.isErrorEnabled()) {
        LOG.error("Agent we advertise (" + agentName
            + ") not listed first in 'http.robots.agents' property!");
      }

      // append all the agents from the http.robots.agents property
      for(; index < agents.size(); index++) {
        combinedAgentsString.append(", " + agents.get(index));
      }

      // always make sure "*" is included in the end
      combinedAgentsString.append(", *");
      this.agentNames = combinedAgentsString.toString();
    }
  }

  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Parses the robots content using the {@link SimpleRobotRulesParser} from crawler commons
   *    
   * @param url A string containing url
   * @param content Contents of the robots file in a byte array 
   * @param contentType The 
   * @param robotName A string containing value of  
   * @return BaseRobotRules object 
   */
  public BaseRobotRules parseRules (String url, byte[] content, String contentType, String robotName) {
    return robotParser.parseContent(url, content, contentType, robotName); 
  }

  public BaseRobotRules getRobotRulesSet(Protocol protocol, String url) {
    URL u = null;
    try {
      u = new URL(url);
    } catch (Exception e) {
      return EMPTY_RULES;
    }
    return getRobotRulesSet(protocol, u);
  }

  public abstract BaseRobotRules getRobotRulesSet(Protocol protocol, URL url);

  /** command-line main for testing */
  public static void main(String[] argv) {

    if (argv.length < 3) {
      System.err.println("Usage: RobotRulesParser <robots-file> <url-file> <agent-names>\n");
      System.err.println("    <robots-file> - Input robots.txt file which will be parsed.");
      System.err.println("    <url-file>    - Contains input URLs (1 per line) which are tested against the rules.");
      System.err.println("    <agent-names> - Input agent name. Multiple agent names can be specified using spaces.");
      System.exit(-1);
    }

    try {
      StringBuilder agentNames = new StringBuilder();
      for(int counter = 2; counter < argv.length; counter++) 
        agentNames.append(argv[counter]).append(",");

      agentNames.deleteCharAt(agentNames.length()-1);

      byte[] robotsBytes = Files.toByteArray(new File(argv[0]));
      BaseRobotRules rules = robotParser.parseContent(argv[0], robotsBytes, "text/plain", agentNames.toString());

      LineNumberReader testsIn = new LineNumberReader(new FileReader(argv[1]));
      String testPath = testsIn.readLine().trim();
      while (testPath != null) {
        System.out.println( (rules.isAllowed(testPath) ? "allowed" : "not allowed") +
            ":\t" + testPath);
        testPath = testsIn.readLine();
      }
      testsIn.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

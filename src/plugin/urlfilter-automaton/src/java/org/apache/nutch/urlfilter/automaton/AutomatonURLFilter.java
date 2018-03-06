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
package org.apache.nutch.urlfilter.automaton;

// JDK imports
import java.io.Reader;
import java.io.IOException;
import java.io.StringReader;
import java.util.regex.PatternSyntaxException;


// Hadoop imports
import org.apache.hadoop.conf.Configuration;


// Automaton imports
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;

import org.apache.nutch.net.*;
import org.apache.nutch.urlfilter.api.RegexRule;
import org.apache.nutch.urlfilter.api.RegexURLFilterBase;

/**
 * RegexURLFilterBase implementation based on the <a
 * href="http://www.brics.dk/automaton/">dk.brics.automaton</a> Finite-State
 * Automata for Java<sup>TM</sup>.
 * 
 * @author J&eacute;r&ocirc;me Charron
 * @see <a href="http://www.brics.dk/automaton/">dk.brics.automaton</a>
 */
public class AutomatonURLFilter extends RegexURLFilterBase {
  public static final String URLFILTER_AUTOMATON_FILE = "urlfilter.automaton.file";
  public static final String URLFILTER_AUTOMATON_RULES = "urlfilter.automaton.rules";

  public AutomatonURLFilter() {
    super();
  }

  public AutomatonURLFilter(String filename) throws IOException,
      PatternSyntaxException {
    super(filename);
  }

  AutomatonURLFilter(Reader reader) throws IOException,
      IllegalArgumentException {
    super(reader);
  }

  /*
   * ----------------------------------- * <implementation:RegexURLFilterBase> *
   * -----------------------------------
   */

  /**
   * Rules specified as a config property will override rules specified as a
   * config file.
   */
  protected Reader getRulesReader(Configuration conf) throws IOException {
    String stringRules = conf.get(URLFILTER_AUTOMATON_RULES);
    if (stringRules != null) {
      return new StringReader(stringRules);
    }
    String fileRules = conf.get(URLFILTER_AUTOMATON_FILE);
    return conf.getConfResourceAsReader(fileRules);
  }

  // Inherited Javadoc
  protected RegexRule createRule(boolean sign, String regex) {
    return new Rule(sign, regex);
  }
  
  protected RegexRule createRule(boolean sign, String regex, String hostOrDomain) {
    return new Rule(sign, regex, hostOrDomain);
  }

  /*
   * ------------------------------------ * </implementation:RegexURLFilterBase>
   * * ------------------------------------
   */

  public static void main(String args[]) throws IOException {
    main(new AutomatonURLFilter(), args);
  }

  private class Rule extends RegexRule {

    private RunAutomaton automaton;

    Rule(boolean sign, String regex) {
      super(sign, regex);
      automaton = new RunAutomaton(new RegExp(regex, RegExp.ALL).toAutomaton());
    }
    
    Rule(boolean sign, String regex, String hostOrDomain) {
      super(sign, regex, hostOrDomain);
      automaton = new RunAutomaton(new RegExp(regex, RegExp.ALL).toAutomaton());
    }

    public boolean match(String url) {
      return automaton.run(url);
    }

	@Override
	public String replace(String url, String replacement) {
		// TODO Auto-generated method stub
		return null;
	}
  }

}

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
package org.apache.nutch.urlfilter.api;

// JDK imports
import java.io.Reader;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;

// Nutch imports
import org.apache.nutch.net.*;


/**
 * Generic {@link org.apache.nutch.net.URLFilter URL filter} based on
 * regular expressions.
 *
 * <p>The regular expressions rules are expressed in a file. The file of rules
 * is provided by each implementation using the
 * {@link #getRulesFile(Configuration)} method.</p>
 * 
 * <p>The format of this file is made of many rules (one per line):<br/>
 * <code>
 * [+-]&lt;regex&gt;
 * </code><br/>
 * where plus (<code>+</code>)means go ahead and index it and minus 
 * (<code>-</code>)means no.</p>
 *
 * @author J&eacute;r&ocirc;me Charron
 */
public abstract class RegexURLFilterBase implements URLFilter {

  /** My logger */
  private final static Log LOG = LogFactory.getLog(RegexURLFilterBase.class);

  /** An array of applicable rules */
  private RegexRule[] rules;

  /** The current configuration */
  private Configuration conf;


  /**
   * Constructs a new empty RegexURLFilterBase
   */
  public RegexURLFilterBase() { }

  /**
   * Constructs a new RegexURLFilter and init it with a file of rules.
   * @param filename is the name of rules file.
   */
  public RegexURLFilterBase(String filename)
    throws IOException, IllegalArgumentException {
    this(new FileReader(filename));
  }

  /**
   * Constructs a new RegexURLFilter and init it with a Reader of rules.
   * @param reader is a reader of rules.
   */
  protected RegexURLFilterBase(Reader reader)
    throws IOException, IllegalArgumentException {
    rules = readRulesFile(reader);
  }
  
  /**
   * Creates a new {@link RegexRule}.
   * @param sign of the regular expression.
   *        A <code>true</code> value means that any URL matching this rule
   *        must be included, whereas a <code>false</code>
   *        value means that any URL matching this rule must be excluded.
   * @param regex is the regular expression associated to this rule.
   */
  protected abstract RegexRule createRule(boolean sign, String regex);
  
  /**
   * Returns the name of the file of rules to use for
   * a particular implementation.
   * @param conf is the current configuration.
   * @return the name of the file of rules to use.
   */
  protected abstract String getRulesFile(Configuration conf);
  
  
  /* -------------------------- *
   * <implementation:URLFilter> *
   * -------------------------- */
  
  // Inherited Javadoc
  public synchronized String filter(String url) {
    for (int i=0; i<rules.length; i++) {
      if (rules[i].match(url)) {
        return rules[i].accept() ? url : null;
      }
    };
    return null;
  }

  /* --------------------------- *
   * </implementation:URLFilter> *
   * --------------------------- */
  
  
  /* ----------------------------- *
   * <implementation:Configurable> *
   * ----------------------------- */
  
  public void setConf(Configuration conf) {
    this.conf = conf;
    String file = getRulesFile(conf);
    Reader reader = conf.getConfResourceAsReader(file);
    if (reader == null) {
      if (LOG.isFatalEnabled()) { LOG.fatal("Can't find resource: " + file); }
    } else {
      try {
        rules = readRulesFile(reader);
      } catch (IOException e) {
        if (LOG.isFatalEnabled()) { LOG.fatal(e.getMessage()); }
        //TODO mb@media-style.com: throw Exception? Because broken api.
        throw new RuntimeException(e.getMessage(), e);
      }
    }
  }

  public Configuration getConf() {
    return this.conf;
  }
  
  /* ------------------------------ *
   * </implementation:Configurable> *
   * ------------------------------ */
  

  /**
   * Read the specified file of rules.
   * @param reader is a reader of regular expressions rules.
   * @return the corresponding {@RegexRule rules}.
   */
  private RegexRule[] readRulesFile(Reader reader)
    throws IOException, IllegalArgumentException {

    BufferedReader in = new BufferedReader(reader);
    List rules = new ArrayList();
    String line;
       
    while((line=in.readLine())!=null) {
      if (line.length() == 0) {
        continue;
      }
      char first=line.charAt(0);
      boolean sign=false;
      switch (first) {
      case '+' : 
        sign=true;
        break;
      case '-' :
        sign=false;
        break;
      case ' ' : case '\n' : case '#' :           // skip blank & comment lines
        continue;
      default :
        throw new IOException("Invalid first character: "+line);
      }

      String regex = line.substring(1);
      if (LOG.isTraceEnabled()) { LOG.trace("Adding rule [" + regex + "]"); }
      RegexRule rule = createRule(sign, regex);
      rules.add(rule);
    }
    return (RegexRule[]) rules.toArray(new RegexRule[rules.size()]);
  }

  /**
   * Filter the standard input using a RegexURLFilterBase.
   * @param filter is the RegexURLFilterBase to use for filtering the
   *        standard input.
   * @param args some optional parameters (not used).
   */
  public static void main(RegexURLFilterBase filter, String args[])
    throws IOException, IllegalArgumentException {

    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;
    while((line=in.readLine())!=null) {
      String out = filter.filter(line);
      if (out!=null) {
        System.out.print("+");
        System.out.println(out);
      } else {
        System.out.print("-");
        System.out.println(line);
      }
    }
  }

}

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

package org.apache.nutch.net.urlnormalizer.regex;

import java.net.URL;
import java.net.MalformedURLException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.nutch.net.URLNormalizer;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.NutchConfiguration;

import javax.xml.parsers.*;
import org.w3c.dom.*;
import org.apache.oro.text.regex.*;

/**
 * Allows users to do regex substitutions on all/any URLs that are encountered,
 * which is useful for stripping session IDs from URLs.
 * 
 * <p>This class uses the <tt>urlnormalizer.regex.file</tt> property.
 * It should be set to the file name of an xml file which should contain the
 * patterns and substitutions to be done on encountered URLs.
 * </p>
 * <p>This class also supports different rules depending on the scope. Please see
 * the javadoc in {@link org.apache.nutch.net.URLNormalizers} for more details.</p>
 * 
 * @author Luke Baker
 * @author Andrzej Bialecki
 */
public class RegexURLNormalizer extends Configured implements URLNormalizer {

  private static final Log LOG = LogFactory.getLog(RegexURLNormalizer.class);

  /**
   * Class which holds a compiled pattern and its corresponding substition
   * string.
   */
  private static class Rule {
    public Perl5Pattern pattern;

    public String substitution;
  }

  private HashMap scopedRules;
  
  private static final List EMPTY_RULES = Collections.EMPTY_LIST;

  private PatternMatcher matcher = new Perl5Matcher();

  /**
   * The default constructor which is called from UrlNormalizerFactory
   * (normalizerClass.newInstance()) in method: getNormalizer()*
   */
  public RegexURLNormalizer() {
    super(null);
  }

  public RegexURLNormalizer(Configuration conf) {
    super(conf);
  }

  /**
   * Constructor which can be passed the file name, so it doesn't look in the
   * configuration files for it.
   */
  public RegexURLNormalizer(Configuration conf, String filename)
          throws IOException, MalformedPatternException {
    super(conf);
    List rules = readConfigurationFile(filename);
    if (rules != null)
      scopedRules.put(URLNormalizers.SCOPE_DEFAULT, rules);
  }

  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) return;
    // the default constructor was called
    if (this.scopedRules == null) {
      String filename = getConf().get("urlnormalizer.regex.file");
      scopedRules = new HashMap();
      URL url = getConf().getResource(filename);
      List rules = null;
      if (url == null) {
        LOG.warn("Can't load the default config file! " + filename);
        rules = EMPTY_RULES;
      } else {
        try {
          rules = readConfiguration(url.openStream());
        } catch (Exception e) {
          LOG.warn("Couldn't read default config from '" + url + "': " + e);
          rules = EMPTY_RULES;
        }
      }
      scopedRules.put(URLNormalizers.SCOPE_DEFAULT, rules);
    }
  }

  // used in JUnit test.
  void setConfiguration(InputStream is, String scope) {
    List rules = readConfiguration(is);
    scopedRules.put(scope, rules);
    LOG.debug("Set config for scope '" + scope + "': " + rules.size() + " rules.");
  }
  
  /**
   * This function does the replacements by iterating through all the regex
   * patterns. It accepts a string url as input and returns the altered string.
   */
  public synchronized String regexNormalize(String urlString, String scope) {
    List curRules = (List)scopedRules.get(scope);
    if (curRules == null) {
      // try to populate
      String configFile = getConf().get("urlnormalizer.regex.file." + scope);
      if (configFile != null) {
        URL resource = getConf().getResource(configFile);
        LOG.debug("resource for scope '" + scope + "': " + resource);
        if (resource == null) {
          LOG.warn("Can't load resource for config file: " + configFile);
        } else {
          try {
            InputStream is = resource.openStream();
            curRules = readConfiguration(resource.openStream());
            scopedRules.put(scope, curRules);
          } catch (Exception e) {
            LOG.warn("Couldn't load resource '" + resource + "': " + e);
          }
        }
      }
      if (curRules == EMPTY_RULES || curRules == null) {
        LOG.warn("can't find rules for scope '" + scope + "', using default");
        scopedRules.put(scope, EMPTY_RULES);
      }
    }
    if (curRules == EMPTY_RULES || curRules == null) {
      // use global rules
      curRules = (List)scopedRules.get(URLNormalizers.SCOPE_DEFAULT);
    }
    Iterator i = curRules.iterator();
    while (i.hasNext()) {
      Rule r = (Rule) i.next();
      urlString = Util.substitute(matcher, r.pattern, new Perl5Substitution(
              r.substitution), urlString, Util.SUBSTITUTE_ALL); // actual
                                                                // substitution
    }
    return urlString;
  }

  public synchronized String normalize(String urlString, String scope)
          throws MalformedURLException {
    return regexNormalize(urlString, scope);
  }

  /** Reads the configuration file and populates a List of Rules. */
  private List readConfigurationFile(String filename) {
    if (LOG.isInfoEnabled()) {
      LOG.info("loading " + filename);
    }
    try {
      FileInputStream fis = new FileInputStream(filename);
      return readConfiguration(fis);
    } catch (Exception e) {
      LOG.fatal("Error loading rules from '" + filename + "': " + e);
      return EMPTY_RULES;
    }
  }
  
  private List readConfiguration(InputStream is) {
    Perl5Compiler compiler = new Perl5Compiler();
    List rules = new ArrayList();
    try {

      // borrowed heavily from code in Configuration.java
      Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
              .parse(is);
      Element root = doc.getDocumentElement();
      if ((!"regex-normalize".equals(root.getTagName()))
              && (LOG.isFatalEnabled())) {
        LOG.fatal("bad conf file: top-level element not <regex-normalize>");
      }
      NodeList regexes = root.getChildNodes();
      for (int i = 0; i < regexes.getLength(); i++) {
        Node regexNode = regexes.item(i);
        if (!(regexNode instanceof Element))
          continue;
        Element regex = (Element) regexNode;
        if ((!"regex".equals(regex.getTagName())) && (LOG.isWarnEnabled())) {
          LOG.warn("bad conf file: element not <regex>");
        }
        NodeList fields = regex.getChildNodes();
        String patternValue = null;
        String subValue = null;
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element) fieldNode;
          if ("pattern".equals(field.getTagName()) && field.hasChildNodes())
            patternValue = ((Text) field.getFirstChild()).getData();
          if ("substitution".equals(field.getTagName())
                  && field.hasChildNodes())
            subValue = ((Text) field.getFirstChild()).getData();
          if (!field.hasChildNodes())
            subValue = "";
        }
        if (patternValue != null && subValue != null) {
          Rule rule = new Rule();
          rule.pattern = (Perl5Pattern) compiler.compile(patternValue);
          rule.substitution = subValue;
          rules.add(rule);
        }
      }
    } catch (Exception e) {
      if (LOG.isFatalEnabled()) {
        LOG.fatal("error parsing conf file: " + e);
      }
      return EMPTY_RULES;
    }
    if (rules.size() == 0) return EMPTY_RULES;
    return rules;
  }

  /** Spits out patterns and substitutions that are in the configuration file. */
  public static void main(String args[]) throws MalformedPatternException,
          IOException {
    RegexURLNormalizer normalizer = new RegexURLNormalizer();
    normalizer.setConf(NutchConfiguration.create());
    Iterator i = ((List)normalizer.scopedRules.get(URLNormalizers.SCOPE_DEFAULT)).iterator();
    System.out.println("* Rules for 'DEFAULT' scope:");
    while (i.hasNext()) {
      Rule r = (Rule) i.next();
      System.out.print("  " + r.pattern.getPattern() + " -> ");
      System.out.println(r.substitution);
    }
    // load the scope
    if (args.length > 1) {
      normalizer.normalize("http://test.com", args[1]);
    }
    if (normalizer.scopedRules.size() > 1) {
      Iterator it = normalizer.scopedRules.keySet().iterator();
      while (it.hasNext()) {
        String scope = (String)it.next();
        if (URLNormalizers.SCOPE_DEFAULT.equals(scope)) continue;
        System.out.println("* Rules for '" + scope + "' scope:");
        i = ((List)normalizer.scopedRules.get(scope)).iterator();
        while (i.hasNext()) {
          Rule r = (Rule) i.next();
          System.out.print("  " + r.pattern.getPattern() + " -> ");
          System.out.println(r.substitution);
        }
      }
    }
    if (args.length > 0) {
      System.out.println("\n---------- Normalizer test -----------");
      String scope = URLNormalizers.SCOPE_DEFAULT;
      if (args.length > 1) scope = args[1];
      System.out.println("Scope: " + scope);
      System.out.println("Input url:  '" + args[0] + "'");
      System.out.println("Output url: '" + normalizer.normalize(args[0], scope) + "'");
    }
    System.exit(0);
  }

}

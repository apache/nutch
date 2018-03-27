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
package org.apache.nutch.urlfilter.ignoreexempt.bidirectional;

import java.io.FileReader;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.net.URLExemptionFilter;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.InputSource;

/**
 * This implementation of {@link org.apache.nutch.net.URLExemptionFilter} uses
 * regex configuration for both fromUrl and toUrl to check if URL is eligible
 * for exemption from 'db.ignore.external'. When this filter is enabled, the
 * external urls will be checked against configured sequence of regex rules.
 * <p>
 * The exemption rule file defaults to
 * db-ignore-external-exemptions-bidirectional.xml in the classpath but can be
 * overridden using the property
 * <code>"db.ignore.external.exemptions.bidirectional.file" in ./conf/nutch-*.xml</code>
 * </p>
 * 
 * @since Mar 1, 2018
 * @version 1
 */
public class BidirectionalExemptionUrlFilter extends Configured implements
    URLExemptionFilter {

  public static final String DB_IGNORE_EXTERNAL_EXEMPTIONS_BIDIRECTIONAL_FILE = "db.ignore.external.exemptions.bidirectional.file";
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles
      .lookup().lookupClass());

  private static class Rule {
    public Pattern pattern;
    public String substitution;
  }

  private List<Rule> rules;

  // private Configuration conf;
  private String fileName;

  public BidirectionalExemptionUrlFilter() {
  }

  public BidirectionalExemptionUrlFilter(Configuration conf) {
    super(conf);
  }

  @Override
  // This implementation checks rules exceptions for two arbitrary urls. True if
  // reg_ex(toUrl) = reg_ex(fromUrl).
  // Logic of reading of RegEx is taken from RegexURLNormalizer
  public boolean filter(String fromUrl, String toUrl) {
    String sourceHost = URLUtil.getHost(fromUrl).toLowerCase();
    String sourceDestination = URLUtil.getHost(toUrl).toLowerCase();

    if (LOG.isDebugEnabled()) {
      LOG.debug("BidirectionalExemptionUrlFilter. Source url: " + fromUrl
          + " and destination url " + toUrl);
    }

    String modifiedSourceHost = sourceHost;
    String modifiedDestinationHost = sourceDestination;

    modifiedSourceHost = this.regexReplace(modifiedSourceHost);
    modifiedDestinationHost = this.regexReplace(modifiedDestinationHost);

    if (modifiedSourceHost == null || modifiedDestinationHost == null) {
      return false;
    }
    return modifiedSourceHost.equals(modifiedDestinationHost);
  }

  private List<Rule> readConfigurationFile() {
    // String filename =
    // this.getConf().get(DB_IGNORE_EXTERNAL_EXEMPTIONS_BIDIRECTIONAL_FILE);

    if (LOG.isInfoEnabled()) {
      LOG.info("loading " + this.fileName);
    }
    try {
      Reader reader = getConf().getConfResourceAsReader(this.fileName);
      return readConfiguration(reader);
    } catch (Exception e) {
      LOG.error("Error loading rules from '" + this.fileName + "': " + e);
      return null;
    }
  }

  private List<Rule> readConfiguration(Reader reader) {
    List<Rule> rules = new ArrayList<Rule>();
    try {

      // borrowed heavily from code in Configuration.java
      Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
          .parse(new InputSource(reader));
      Element root = doc.getDocumentElement();
      if ((!"regex-exemptionurl".equals(root.getTagName()))
          && (LOG.isErrorEnabled())) {
        LOG.error("bad conf file: top-level element not <regex-exemptionurl>");
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
          try {
            rule.pattern = Pattern.compile(patternValue);
          } catch (PatternSyntaxException e) {
            if (LOG.isErrorEnabled()) {
              LOG.error("skipped rule: " + patternValue + " -> " + subValue
                  + " : invalid regular expression pattern: " + e);
            }
            continue;
          }
          rule.substitution = subValue;
          rules.add(rule);
        }
      }
    } catch (Exception e) {
      if (LOG.isErrorEnabled()) {
        LOG.error("error parsing conf file: " + e);
      }
      return null;
    }
    if (rules.size() == 0)
      return null;
    return rules;
  }

  private String regexReplace(String urlString) {
    if (rules == null) {
      return null;
    }

    Iterator<Rule> i = rules.iterator();
    while (i.hasNext()) {
      Rule r = (Rule) i.next();
      Matcher matcher = r.pattern.matcher(urlString);
      urlString = matcher.replaceAll(r.substitution);
    }
    return urlString;
  }

  public static void main(String[] args) {

    if (args.length != 2) {
      System.out.println("Error: Invalid Args");
      System.out.println("Usage: "
          + BidirectionalExemptionUrlFilter.class.getName()
          + " <url_source, url_destination>");
      return;
    }
    String sourceUrl = args[0];
    String destinationUrl = args[1];
    BidirectionalExemptionUrlFilter instance = new BidirectionalExemptionUrlFilter(
        NutchConfiguration.create());
    System.out.println(instance.filter(sourceUrl, destinationUrl));
  }

  /*
   * @Override public Configuration getConf() { return super.getConf(); }
   */
  @Override
  public void setConf(Configuration conf) {
    if (conf == null) {
      return;
    }
    super.setConf(conf);
    this.fileName = conf.get(DB_IGNORE_EXTERNAL_EXEMPTIONS_BIDIRECTIONAL_FILE);
    this.rules = readConfigurationFile();
  }

}

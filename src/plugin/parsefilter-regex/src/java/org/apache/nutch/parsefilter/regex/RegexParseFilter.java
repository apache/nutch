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

package org.apache.nutch.parsefilter.regex;

import java.lang.invoke.MethodHandles;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.protocol.Content;

import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.w3c.dom.*;

/**
 * RegexParseFilter. If a regular expression matches either HTML or 
 * extracted text, a configurable field is set to true.
 */
public class RegexParseFilter implements HtmlParseFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  private static String attributeFile = null;
  private String regexFile = null;
  
  private Configuration conf;
  private DocumentFragment doc;
  
  private static final Map<String,RegexRule> rules = new HashMap<String,RegexRule>();
  
  public RegexParseFilter() {}
  
  public RegexParseFilter(String regexFile) {
    this.regexFile = regexFile;
  }

  public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {
    Parse parse = parseResult.get(content.getUrl());
    String html = new String(content.getContent());
    String text = parse.getText();
    
    for (Map.Entry<String, RegexRule> entry : rules.entrySet()) {
      String field = entry.getKey();
      RegexRule regexRule = entry.getValue();
      
      String source = null;
      if (regexRule.source.equalsIgnoreCase("html")) {
        source = html;
      }
      if (regexRule.source.equalsIgnoreCase("text")) {
        source = text;
      }
      
      if (source == null) {
        LOG.error("source for regex rule: " + field + " misconfigured");
      }
      
      if (matches(source, regexRule.regex)) {
        parse.getData().getParseMeta().set(field, "true");
      } else {
        parse.getData().getParseMeta().set(field, "false");
      }
    }
    
    return parseResult;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;

    // get the extensions for domain urlfilter
    String pluginName = "parsefilter-regex";
    Extension[] extensions = PluginRepository.get(conf).getExtensionPoint(
      HtmlParseFilter.class.getName()).getExtensions();
    for (int i = 0; i < extensions.length; i++) {
      Extension extension = extensions[i];
      if (extension.getDescriptor().getPluginId().equals(pluginName)) {
        attributeFile = extension.getAttribute("file");
        break;
      }
    }

    // handle blank non empty input
    if (attributeFile != null && attributeFile.trim().equals("")) {
      attributeFile = null;
    }

    if (attributeFile != null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Attribute \"file\" is defined for plugin " + pluginName
          + " as " + attributeFile);
      }
    }
    else {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Attribute \"file\" is not defined in plugin.xml for plugin "
          + pluginName);
      }
    }

    // domain file and attribute "file" take precedence if defined
    String file = conf.get("parsefilter.regex.file");
    String stringRules = conf.get("parsefilter.regex.rules");
    if (regexFile != null) {
      file = regexFile;
    }
    else if (attributeFile != null) {
      file = attributeFile;
    }
    Reader reader = null;
    if (stringRules != null) { // takes precedence over files
      reader = new StringReader(stringRules);
    } else {
      reader = conf.getConfResourceAsReader(file);
    }
    try {
      if (reader == null) {
        reader = new FileReader(file);
      }
      readConfiguration(reader);
    }
    catch (IOException e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  public Configuration getConf() {
    return this.conf;
  }
  
  private boolean matches(String value, Pattern pattern) {
    if (value != null) {
      Matcher matcher = pattern.matcher(value);
      return matcher.find();
    }
       
    return false;
  }
  
  private synchronized void readConfiguration(Reader configReader) throws IOException {
    if (rules.size() > 0) {
      return;
    }

    String line;
    BufferedReader reader = new BufferedReader(configReader);
    while ((line = reader.readLine()) != null) {
      if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
        line = line.trim();
        String[] parts = line.split("\t");

        String field = parts[0].trim();
        String source = parts[1].trim();
        String regex = parts[2].trim();
        
        rules.put(field, new RegexRule(source, regex));
      }
    }
  }
  
  private static class RegexRule {
    public RegexRule(String source, String regex) {
      this.source = source;
      this.regex = Pattern.compile(regex);
    }
    String source;
    Pattern regex;
  }
}
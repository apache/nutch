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
package org.apache.nutch.net.urlnormalizer.slash;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.URLNormalizer;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.util.URLUtil;

/**
 * @author markus@openindex.io
 */
public class SlashURLNormalizer implements URLNormalizer {

  private Configuration conf;

  private static final Logger LOG = LoggerFactory.getLogger(SlashURLNormalizer.class);

  private static final char QUESTION_MARK = '?';
  private static final char SLASH = '/';
  private static final char DOT = '.';
  private static final String PROTOCOL_DELIMITER = "://";

  private static String attributeFile = null;
  private String slashesFile = null;
  
  // We record a map of hosts and boolean, the boolean denotes whether the host should
  // have slashes after URL paths. True means slash, false means remove the slash
  private static final Map<String,Boolean> slashesMap = new HashMap<String,Boolean>();

  public SlashURLNormalizer() {}

  public SlashURLNormalizer(String slashesFile) {
    this.slashesFile = slashesFile;
  }

  private synchronized void readConfiguration(Reader configReader) throws IOException {
    if (slashesMap.size() > 0) {
      return;
    }

    BufferedReader reader = new BufferedReader(configReader);
    String line, host;
    String rule;
    int delimiterIndex;

    while ((line = reader.readLine()) != null) {
      if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
        line.trim();
        delimiterIndex = line.indexOf(" ");
        // try tabulator
        if (delimiterIndex == -1) {
          delimiterIndex = line.indexOf("\t");
        }

        host = line.substring(0, delimiterIndex);
        rule = line.substring(delimiterIndex + 1).trim();
        
        if (rule.equals("+")) {
          slashesMap.put(host, true);
        } else {
          slashesMap.put(host, false);
        }
      }
    }
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;

    // get the extensions for domain urlfilter
    String pluginName = "urlnormalizer-slash";
    Extension[] extensions = PluginRepository.get(conf).getExtensionPoint(
      URLNormalizer.class.getName()).getExtensions();
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
    String file = conf.get("urlnormalizer.slashes.file");
    String stringRules = conf.get("urlnormalizer.slashes.rules");
    if (slashesFile != null) {
      file = slashesFile;
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
  
  public String normalize(String url, String scope) throws MalformedURLException {
    return normalize(url, null, scope);
  }

  public String normalize(String url, CrawlDatum crawlDatum, String scope) throws MalformedURLException {
    // Get URL repr.
    URL u = new URL(url);
    
    // Get the host
    String host = u.getHost();

    // Do we have a rule for this host?
    if (slashesMap.containsKey(host)) {
      // Yes, separate the path and optional querystring
      String protocol = u.getProtocol();
      String path = u.getPath();

      // Don't do anything to root URL's
      // / is always set by basic normalizer
      if (path.length() > 1) {
        String queryString = u.getQuery();
        
        // Get the rule
        boolean rule = slashesMap.get(host);
        
        // Does it have a trailing slash
        int lastIndexOfSlash = path.lastIndexOf(SLASH);
        boolean trailingSlash = (lastIndexOfSlash == path.length() - 1);
        
        // Do we need to add a trailing slash?
        if (!trailingSlash && rule) {
          // Only add a trailing slash if this path doesn't appear to have an extension/suffix such as .html
          int lastIndexOfDot = path.lastIndexOf(DOT);
          if (path.length() < 6 || lastIndexOfDot == -1 || lastIndexOfDot < path.length() - 6) {          
            StringBuilder buffer = new StringBuilder(protocol);
            buffer.append(PROTOCOL_DELIMITER);
            buffer.append(host);
            buffer.append(path);
            buffer.append(SLASH);
            if (queryString != null) {
              buffer.append(QUESTION_MARK);
              buffer.append(queryString);
            }
            url = buffer.toString();
          }
        }
        
        // Do we need to remove a trailing slash?
        else if (trailingSlash && !rule) {
          StringBuilder buffer = new StringBuilder(protocol);
          buffer.append(PROTOCOL_DELIMITER);
          buffer.append(host);
          buffer.append(path.substring(0, lastIndexOfSlash));
          if (queryString != null) {
            buffer.append(QUESTION_MARK);
            buffer.append(queryString);
          }
          url = buffer.toString();      
        }
      }
    }

    return url;
  }
}
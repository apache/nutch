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
package org.apache.nutch.urlfilter.domain;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLFilter;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.util.URLUtil;
import org.apache.nutch.util.domain.DomainSuffix;

/**
 * <p>Filters URLs based on a file containing domain suffixes, domain names, and
 * hostnames. Only a url that matches one of the suffixes, domains, or hosts
 * present in the file is allowed.</p>
 * 
 * <p>Urls are checked in order of domain suffix, domain name, and hostname
 * against entries in the domain file. The domain file would be setup as follows
 * with one entry per line:
 * 
 * <pre> com apache.org www.apache.org </pre>
 * 
 * <p>The first line is an example of a filter that would allow all .com
 * domains. The second line allows all urls from apache.org and all of its
 * subdomains such as lucene.apache.org and hadoop.apache.org. The third line
 * would allow only urls from www.apache.org. There is no specific ordering to
 * entries. The entries are from more general to more specific with the more
 * general overridding the more specific.</p>
 * 
 * The domain file defaults to domain-urlfilter.txt in the classpath but can be
 * overridden using the:
 * 
 * <ul> <ol>property "urlfilter.domain.file" in ./conf/nutch-*.xml, and</ol>
 * <ol>attribute "file" in plugin.xml of this plugin</ol> </ul>
 * 
 * the attribute "file" has higher precedence if defined.
 */
public class DomainURLFilter
  implements URLFilter {

  private static final Logger LOG = LoggerFactory.getLogger(DomainURLFilter.class);

  // read in attribute "file" of this plugin.
  private static String attributeFile = null;
  private Configuration conf;
  private String domainFile = null;
  private Set<String> domainSet = new LinkedHashSet<String>();

  private void readConfiguration(Reader configReader)
    throws IOException {

    // read the configuration file, line by line
    BufferedReader reader = new BufferedReader(configReader);
    String line = null;
    while ((line = reader.readLine()) != null) {
      if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
        // add non-blank lines and non-commented lines
        domainSet.add(StringUtils.lowerCase(line));
      }
    }
  }

  /**
   * Default constructor.
   */
  public DomainURLFilter() {

  }

  /**
   * Constructor that specifies the domain file to use.
   * 
   * @param domainFile The domain file, overrides domain-urlfilter.text default.
   * 
   * @throws IOException
   */
  public DomainURLFilter(String domainFile) {
    this.domainFile = domainFile;
  }

  /**
   * Sets the configuration.
   */
  public void setConf(Configuration conf) {
    this.conf = conf;

    // get the extensions for domain urlfilter
    String pluginName = "urlfilter-domain";
    Extension[] extensions = PluginRepository.get(conf).getExtensionPoint(
      URLFilter.class.getName()).getExtensions();
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
    String file = conf.get("urlfilter.domain.file");    
    String stringRules = conf.get("urlfilter.domain.rules");
    if (domainFile != null) {
      file = domainFile;
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

  public String filter(String url) {

    try {

      // match for suffix, domain, and host in that order.  more general will
      // override more specific
      String domain = URLUtil.getDomainName(url).toLowerCase().trim();
      String host = URLUtil.getHost(url);
      String suffix = null;
      DomainSuffix domainSuffix = URLUtil.getDomainSuffix(url);
      if (domainSuffix != null) {
        suffix = domainSuffix.getDomain();
      }
      
      if (domainSet.contains(suffix) || domainSet.contains(domain)
        || domainSet.contains(host)) {
        return url;
      }

      // doesn't match, don't allow
      return null;
    }
    catch (Exception e) {
      
      // if an error happens, allow the url to pass
      LOG.error("Could not apply filter on url: " + url + "\n"
        + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return null;
    }
  }
}

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
package org.apache.nutch.net.urlnormalizer.host;

import java.lang.invoke.MethodHandles;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLNormalizer;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;

/**
 * URL normalizer for mapping hosts to their desired form. It takes a simple
 * text file as source in the format:
 * 
 * example.org www.example.org
 * 
 * mapping all URL's of example.org the the www sub-domain. It also allows for
 * wildcards to be used to map all sub-domains to another host:
 * 
 * *.example.org www.example.org
 */
public class HostURLNormalizer implements URLNormalizer {

  private Configuration conf;

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static String attributeFile = null;
  private static final HashMap<String, String> hostsMap = new HashMap<String, String>();

  private synchronized void readConfiguration(Reader configReader)
      throws IOException {
    if (hostsMap.size() > 0) {
      return;
    }

    BufferedReader reader = new BufferedReader(configReader);
    String line, host, target;
    int delimiterIndex;

    while ((line = reader.readLine()) != null) {
      if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
        line = line.trim();
        delimiterIndex = line.indexOf(" ");

        host = line.substring(0, delimiterIndex);
        target = line.substring(delimiterIndex + 1);
        hostsMap.put(host, target);
      }
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    // get the extensions for domain urlfilter
    String pluginName = "urlnormalizer-host";
    Extension[] extensions = PluginRepository.get(conf)
        .getExtensionPoint(URLNormalizer.class.getName()).getExtensions();
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
    } else {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Attribute \"file\" is not defined in plugin.xml for plugin "
            + pluginName);
      }
    }

    // precedence hierarchy for definition of normalizer rules
    // (first non-empty definition takes precedence):
    // 1. string rules defined by `urlnormalizer.hosts.rules`
    // 2. rule file name defined by `urlnormalizer.hosts.file"`
    // 3. rule file name defined in plugin.xml (`attributeFile`)
    String file = conf.get("urlnormalizer.hosts.file", attributeFile);
    String stringRules = conf.get("urlnormalizer.hosts.rules");
    Reader reader = null;
    if (stringRules != null) { // takes precedence over files
      reader = new StringReader(stringRules);
    } else {
      LOG.info("Reading {} rules file {}", pluginName, file);
      reader = conf.getConfResourceAsReader(file);
    }
    try {
      if (reader == null) {
        reader = new FileReader(file);
      }
      readConfiguration(reader);
    } catch (IOException e) {
      LOG.error("Error reading " + pluginName + " rule file " + file, e);
    }
  }

  @Override
  public String normalize(String urlString, String scope)
      throws MalformedURLException {
    String host = new URL(urlString).getHost();

    // Test static hosts
    if (hostsMap.containsKey(host)) {
      return replaceHost(urlString, host, hostsMap.get(host));
    }

    // Test for wildcard in reverse order
    String[] hostParts = host.split("\\.");

    // Use a buffer for our host parts
    StringBuilder hostBuffer = new StringBuilder();

    // This is our temp buffer keeping host parts with a wildcard
    String wildCardHost = new String();

    // Add the tld to the buffer
    hostBuffer.append(hostParts[hostParts.length - 1]);

    for (int i = hostParts.length - 2; i > 0; i--) {
      // Prepend another sub domain
      hostBuffer.insert(0, hostParts[i] + ".");

      // Make a wildcarded sub domain
      wildCardHost = "*." + hostBuffer.toString();

      // Check if this wildcard sub domain exists
      if (hostsMap.containsKey(wildCardHost)) {
        // Replace the original input host with the wildard replaced
        return replaceHost(urlString, host, hostsMap.get(wildCardHost));
      }
    }

    return urlString;
  }

  protected String replaceHost(String urlString, String host, String target) {
    int hostIndex = urlString.indexOf(host);

    StringBuilder buffer = new StringBuilder();

    buffer.append(urlString.substring(0, hostIndex));
    buffer.append(target);
    buffer.append(urlString.substring(hostIndex + host.length()));

    return buffer.toString();
  }

}

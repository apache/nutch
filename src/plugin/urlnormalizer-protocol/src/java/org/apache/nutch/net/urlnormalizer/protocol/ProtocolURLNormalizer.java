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
package org.apache.nutch.net.urlnormalizer.protocol;

import java.lang.invoke.MethodHandles;
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

/**
 * @author markus@openindex.io
 */
public class ProtocolURLNormalizer implements URLNormalizer {

  private Configuration conf;

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final char QUESTION_MARK = '?';
  private static final String PROTOCOL_DELIMITER = "://";

  private static String attributeFile = null;
  private String protocolsFile = null;
  
  // We record a map of hosts and boolean, the boolean denotes whether the host should
  // have slashes after URL paths. True means slash, false means remove the slash
  private static final Map<String,String> protocolsMap = new HashMap<String,String>();

  public ProtocolURLNormalizer() {}

  public ProtocolURLNormalizer(String protocolsFile) {
    this.protocolsFile = protocolsFile;
  }

  private synchronized void readConfiguration(Reader configReader) throws IOException {
    if (protocolsMap.size() > 0) {
      return;
    }

    BufferedReader reader = new BufferedReader(configReader);
    String line, host;
    String protocol;
    int delimiterIndex;

    while ((line = reader.readLine()) != null) {
      if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
        line = line.trim();
        delimiterIndex = line.indexOf(" ");
        // try tabulator
        if (delimiterIndex == -1) {
          delimiterIndex = line.indexOf("\t");
        }

        host = line.substring(0, delimiterIndex);
        protocol = line.substring(delimiterIndex + 1).trim();
        
        protocolsMap.put(host, protocol);
      }
    }
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;

    // get the extensions for domain urlfilter
    String pluginName = "urlnormalizer-protocol";
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
    String file = conf.get("urlnormalizer.protocols.file");
    String stringRules = conf.get("urlnormalizer.protocols.rules");
    if (protocolsFile != null) {
      file = protocolsFile;
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
    if (protocolsMap.containsKey(host)) {    
      String protocol = u.getProtocol();
      String requiredProtocol = protocolsMap.get(host);
      
      // Incorrect protocol?
      if (!protocol.equals(requiredProtocol)) {
        // Rebuild URL with new protocol
        StringBuilder buffer = new StringBuilder(requiredProtocol);
        buffer.append(PROTOCOL_DELIMITER);
        buffer.append(host);
        buffer.append(u.getPath());
        
        String queryString = u.getQuery();
        if (queryString != null) {
          buffer.append(QUESTION_MARK);
          buffer.append(queryString);
        }
        
        url = buffer.toString();
      }
    }

    return url;
  }
}

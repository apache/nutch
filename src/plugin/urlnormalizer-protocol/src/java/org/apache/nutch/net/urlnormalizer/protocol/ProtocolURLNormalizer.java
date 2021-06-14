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
package org.apache.nutch.net.urlnormalizer.protocol;

import java.lang.invoke.MethodHandles;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.URLNormalizer;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.util.SuffixStringMatcher;

/**
 * URL normalizer to normalize the protocol for all URLs of a given host or
 * domain, e.g. normalize <code>http://nutch.apache.org/path/</code> to
 * <code>https://www.apache.org/path/</code> if it's known that the host
 * <code>nutch.apache.org</code> supports https and http-URLs either cause
 * duplicate content or are redirected to https.
 * 
 * See {@link org.apache.nutch.net.urlnormalizer.protocol} for details and
 * configuration.
 */
public class ProtocolURLNormalizer implements URLNormalizer {

  private Configuration conf;

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private String attributeFile = null;
  
  // We record a map of hosts and the protocol string to be used for this host
  private final Map<String,String> protocolsMap = new HashMap<>();

  // Unify protocol strings to reduce the memory footprint (usually there are only
  // two values (http and https)
  private final Map<String,String> protocols = new TreeMap<>();

  // Map of domain suffixes and protocol to be used for all hosts below this domain
  private final Map<String,String> domainProtocolsMap = new HashMap<>();

  // Matcher for domain suffixes
  private SuffixStringMatcher domainMatcher = null;

  // validator for protocols/schemes following RFC 1630
  private final static Pattern PROTOCOL_VALIDATOR = Pattern.compile(
      "^[a-z](?:[a-z0-9$\\-_@.&!*\"'(),]|%[0-9a-f]{2})*$",
      Pattern.CASE_INSENSITIVE);

  private synchronized void readConfiguration(Reader configReader) throws IOException {
    if (protocolsMap.size() > 0) {
      return;
    }

    BufferedReader reader = new BufferedReader(configReader);
    String line, host;
    String protocol;
    int delimiterIndex;
    int lineNumber = 0;

    while ((line = reader.readLine()) != null) {
      lineNumber++;
      line = line.trim();
      if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
        delimiterIndex = line.indexOf(" ");
        // try tabulator
        if (delimiterIndex == -1) {
          delimiterIndex = line.indexOf("\t");
        }
        if (delimiterIndex == -1) {
          LOG.warn("Invalid line {}, no delimiter between <host/domain> and <protocol> found: {}", lineNumber, line);
          continue;
        }

        host = line.substring(0, delimiterIndex);
        protocol = line.substring(delimiterIndex + 1).trim();

        if (!PROTOCOL_VALIDATOR.matcher(protocol).matches()) {
          LOG.warn("Skipping rule with protocol not following RFC 1630 in line {}: {}",
              lineNumber, line);
          continue;
        }

        /*
         * dedup protocol values to reduce memory footprint of map: equal
         * strings are represented by the same string object
         */
        protocols.putIfAbsent(protocol, protocol);
        protocol = protocols.get(protocol);

        if (host.startsWith("*.")) {
          // domain pattern (eg. "*.example.com"):
          // - use ".example.com" for suffix matching,
          //   including the leading dot to avoid mismatches
          //   ("www.myexample.com")
          domainProtocolsMap.put(host.substring(1), protocol);
          // but also match the bare domain name "example.com"
          protocolsMap.put(host.substring(2), protocol);
        } else {
          protocolsMap.put(host, protocol);
        }
      }
    }
    if (domainProtocolsMap.size() > 0) {
      domainMatcher = new SuffixStringMatcher(domainProtocolsMap.keySet());
    }
    LOG.info("Configuration file read: rules for {} hosts and {} domains",
        protocolsMap.size(), domainProtocolsMap.size());
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
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
    if (attributeFile != null && attributeFile.trim().isEmpty()) {
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

    // precedence hierarchy for definition of normalizer rules
    // (first non-empty definition takes precedence):
    // 1. string rules defined by `urlnormalizer.protocols.rules`
    // 2. rule file name defined by `urlnormalizer.protocols.file"`
    // 3. rule file name defined in plugin.xml (`attributeFile`)
    String file = conf.get("urlnormalizer.protocols.file", attributeFile);
    String stringRules = conf.get("urlnormalizer.protocols.rules");
    Reader reader = null;
    if (stringRules != null && !stringRules.isEmpty()) { // takes precedence over files
      reader = new StringReader(stringRules);
    } else {
      LOG.info("Reading {} rules file {} from Java class path", pluginName, file);
      reader = conf.getConfResourceAsReader(file);
    }
    try {
      if (reader == null) {
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(conf);
        LOG.info("Reading {} rules file {}", pluginName, path.toUri());
        reader = new InputStreamReader(fs.open(path));
      }
      readConfiguration(reader);
    } catch (IOException | IllegalArgumentException e) {
      LOG.error("Error reading " + pluginName + " rule file " + file, e);
    }
  }
  
  @Override
  public String normalize(String url, String scope) throws MalformedURLException {
    // Get URL repr.
    URL u = new URL(url);
    
    // Get the host
    String host = u.getHost();

    // Is there a (non-default) port set?
    if (u.getPort() != -1) {
      // do not change the protocol if the port is set
      return url;
    }

    String requiredProtocol = null;

    // Do we have a rule for this host?
    if (protocolsMap.containsKey(host)) {
      requiredProtocol = protocolsMap.get(host);
    } else if (domainMatcher != null) {
      String domainMatch = domainMatcher.longestMatch(host);
      if (domainMatch != null) {
        requiredProtocol = domainProtocolsMap.get(domainMatch);
     }
    }

    // Incorrect protocol?
    if (requiredProtocol != null && !u.getProtocol().equals(requiredProtocol)) {
      // Rebuild URL with new protocol
      url = new URL(requiredProtocol, host, u.getPort(), u.getFile())
          .toString();
    }

    return url;
  }
}

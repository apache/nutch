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
package org.apache.nutch.protocol;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.util.ObjectCache;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates and caches {@link Protocol} plugins. Protocol plugins should define
 * the attribute "protocolName" with the name of the protocol that they
 * implement. Configuration object is used for caching. Cache key is constructed
 * from appending protocol name (eg. http) to constant
 * {@link Protocol#X_POINT_ID}.
 */
public class ProtocolFactory {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private ExtensionPoint extensionPoint;

  private Configuration conf;

  protected Map<String, String> defaultProtocolImplMapping = new HashMap<>();
  protected Map<String, String> hostProtocolMapping = new HashMap<>();

  public ProtocolFactory(Configuration conf) {
    this.conf = conf;
    this.extensionPoint = PluginRepository.get(conf).getExtensionPoint(
        Protocol.X_POINT_ID);
    if (this.extensionPoint == null) {
      throw new RuntimeException("x-point " + Protocol.X_POINT_ID
          + " not found.");
    }

    try {
      BufferedReader reader = new BufferedReader(conf.getConfResourceAsReader("host-protocol-mapping.txt"));
      String line;
      String parts[];
      while ((line = reader.readLine()) != null) {
        if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
          line = line.trim();
          parts = line.split("\t");

          // Must be at least two parts
          if (parts.length == 2) {
            // Is this a host to plugin mapping, or a default?
            if (parts[0].indexOf(":") == -1) {
              hostProtocolMapping.put(parts[0].trim(), parts[1].trim());
            } else {
              String[] moreParts = parts[0].split(":");
              defaultProtocolImplMapping.put(moreParts[1].trim(), parts[1].trim());
            }
          } else {
            LOG.warn("Wrong format of line: {}", line);
            LOG.warn("Expected format: <hostname> <tab> <plugin_id> or protocol:<protocol> <tab> <plugin_id>");
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Unable to read host-protocol-mapping.txt", e);
    }
  }
  /**
   * Returns the appropriate {@link Protocol} implementation for a url.
   * 
   * @param urlString
   *          Url String
   * @return The appropriate {@link Protocol} implementation for a given
   *         {@link URL}.
   * @throws ProtocolNotFound
   *           when Protocol can not be found for urlString or urlString is not
   *           a valid URL
   */
  public Protocol getProtocol(String urlString) throws ProtocolNotFound {
    try {
      URL url = new URL(urlString);
      return getProtocol(url);
    } catch (MalformedURLException e) {
      throw new ProtocolNotFound(urlString, e.toString());
    }
  }

  /**
   * Returns the appropriate {@link Protocol} implementation for a url.
   * 
   * @param url
   *          URL to be fetched by returned {@link Protocol} implementation
   * @return The appropriate {@link Protocol} implementation for a given
   *         {@link URL}.
   * @throws ProtocolNotFound
   *           when Protocol can not be found for url
   */
  public Protocol getProtocol(URL url)
      throws ProtocolNotFound {
    try {
      Protocol protocol = null;

      // First attempt to resolve a protocol implementation by hostname
      String host = url.getHost();
      if (hostProtocolMapping.containsKey(host)) {
        Extension extension = getExtensionById(hostProtocolMapping.get(host));
        if (extension != null) {
          protocol = getProtocolInstanceByExtension(extension);
        }
      }

      // Nothing, see if we have defaults configured
      if (protocol == null) {
        // Protocol listed in default map?
        if (defaultProtocolImplMapping.containsKey(url.getProtocol())) {
          Extension extension = getExtensionById(defaultProtocolImplMapping.get(url.getProtocol()));
          if (extension != null) {
            protocol = getProtocolInstanceByExtension(extension);
          }
        }
      }

      // Still couldn't find a protocol? Attempt by protocol
      if (protocol == null) {
        Extension extension = findExtension(url.getProtocol(), "protocolName");
        if (extension != null) {
          protocol = getProtocolInstanceByExtension(extension);
        }
      }

      // Got anything?
      if (protocol != null) {
        return protocol;
      }

      // Nothing!
      throw new ProtocolNotFound(url.toString());
    } catch (PluginRuntimeException e) {
      throw new ProtocolNotFound(url.toString(), e.toString());
    }
  }

  private Protocol getProtocolInstanceByExtension(Extension extension) throws PluginRuntimeException {
    Protocol protocol = null;
    String cacheId = extension.getId();
    ObjectCache objectCache = ObjectCache.get(conf);
    synchronized (objectCache) {
      if (!objectCache.hasObject(cacheId)) {
        protocol = (Protocol) extension.getExtensionInstance();
        objectCache.setObject(cacheId, protocol);
      }
      protocol = (Protocol) objectCache.getObject(cacheId);
    }

    return protocol;
  }

  private Extension getExtensionById(String id) {
    Extension[] extensions = this.extensionPoint.getExtensions();
    for (int i = 0; i < extensions.length; i++) {
      if (id.equals(extensions[i].getId())) {
        return extensions[i];
      }
    }

    return null;
  }

  private Extension findExtension(String name, String attribute) throws PluginRuntimeException {
    for (int i = 0; i < this.extensionPoint.getExtensions().length; i++) {
      Extension extension = this.extensionPoint.getExtensions()[i];

      if (contains(name, extension.getAttribute(attribute)))
        return extension;
    }
    return null;
  }

  boolean contains(String what, String where) {
    if (where != null) {
      String parts[] = where.split("[, ]");
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].equals(what))
          return true;
      }
    }
    return false;
  }

}

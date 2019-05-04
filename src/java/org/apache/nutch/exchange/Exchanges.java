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
package org.apache.nutch.exchange;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

public class Exchanges {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Map<String, ExchangeConfigRelation> exchanges;

  private ExchangeConfig defaultExchangeConfig;

  private boolean availableExchanges = true;

  public Exchanges(Configuration conf) {
    try {
      ExtensionPoint point = PluginRepository.get(conf)
          .getExtensionPoint(Exchange.X_POINT_ID);
      if (point == null) {
        throw new RuntimeException(Exchange.X_POINT_ID + " not found.");
      }

      HashMap<String, Extension> extensionMap = new HashMap<>();
      for (Extension extension : point.getExtensions()) {
        extensionMap.putIfAbsent(extension.getClazz(), extension);
      }

      exchanges = new HashMap<>();

      ExchangeConfig[] exchangeConfigs = loadConfigurations(conf);

      for (ExchangeConfig exchangeConfig : exchangeConfigs) {
        final String clazz = exchangeConfig.getClazz();

        // If was enabled in plugin.includes property
        if (extensionMap.containsKey(clazz)) {
          ExchangeConfigRelation exchangeConfigRelation = new ExchangeConfigRelation(
              (Exchange) extensionMap.get(clazz).getExtensionInstance(),
              exchangeConfig);
          exchanges.put(exchangeConfig.getId(), exchangeConfigRelation);
        }
      }

      if (exchanges.isEmpty() && defaultExchangeConfig == null) {
        availableExchanges = false;
        LOG.warn("No exchange was configured. The documents will be routed to all index writers.");
      }
    } catch (PluginRuntimeException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean areAvailableExchanges() {
    return availableExchanges;
  }

  /**
   * Loads the configuration of each exchange.
   *
   * @param conf Nutch's configuration.
   * @return An array with each exchange's configuration.
   */
  private ExchangeConfig[] loadConfigurations(Configuration conf) {
    InputSource inputSource = new InputSource(
        conf.getConfResourceAsInputStream("exchanges.xml"));

    final List<ExchangeConfig> configList = new LinkedList<>();

    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Element rootElement = builder.parse(inputSource).getDocumentElement();
      NodeList exchangeList = rootElement.getElementsByTagName("exchange");

      for (int i = 0; i < exchangeList.getLength(); i++) {
        Element element = (Element) exchangeList.item(i);
        ExchangeConfig exchangeConfig = ExchangeConfig.getInstance(element);

        if ("default".equals(exchangeConfig.getClazz())) {
          this.defaultExchangeConfig = exchangeConfig;
          continue;
        }

        configList.add(exchangeConfig);
      }

    } catch (SAXException | IOException | ParserConfigurationException e) {
      LOG.warn(e.toString());
    }

    return configList.toArray(new ExchangeConfig[0]);
  }

  /**
   * Opens each configured exchange.
   */
  public void open() {
    exchanges.forEach(
        (id, value) -> value.exchange.open(value.config.getParameters()));
  }

  /**
   * Returns all the indexers where the document must be sent to.
   *
   * @param nutchDocument The document to process.
   * @return Indexers.
   */
  public String[] indexWriters(final NutchDocument nutchDocument) {
    final Set<String> writersIDs = new HashSet<>();

    exchanges.forEach((id, value) -> {
      if (value.exchange.match(nutchDocument)) {
        writersIDs.addAll(Arrays.asList(value.config.getWritersIDs()));
      }
    });

    // Using the default exchange if it's activated and there is not index writers for this document yet.
    if (defaultExchangeConfig != null && writersIDs.isEmpty()) {
      return defaultExchangeConfig.getWritersIDs();
    }

    return writersIDs.toArray(new String[0]);
  }

  /**
   * Wrapper for a single exchange and its configuration.
   */
  private class ExchangeConfigRelation {

    private final Exchange exchange;

    private final ExchangeConfig config;

    ExchangeConfigRelation(Exchange exchange, ExchangeConfig config) {
      this.exchange = exchange;
      this.config = config;
    }
  }
}

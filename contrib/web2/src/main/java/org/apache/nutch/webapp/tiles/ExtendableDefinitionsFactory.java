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
package org.apache.nutch.webapp.tiles;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.ServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.webapp.common.PluginResourceLoader;
import org.apache.nutch.webapp.common.ServletContextServiceLocator;
import org.apache.nutch.webapp.common.Startable;
import org.apache.nutch.webapp.extension.UIExtensionPoint;
import org.apache.struts.tiles.ComponentDefinition;
import org.apache.struts.tiles.Controller;
import org.apache.struts.tiles.DefinitionsFactory;
import org.apache.struts.tiles.DefinitionsFactoryConfig;
import org.apache.struts.tiles.DefinitionsFactoryException;
import org.apache.struts.tiles.NoSuchDefinitionException;
import org.apache.struts.tiles.xmlDefinition.XmlDefinition;
import org.apache.struts.tiles.xmlDefinition.XmlDefinitionsSet;
import org.apache.struts.tiles.xmlDefinition.XmlParser;

import org.xml.sax.SAXException;

/**
 * Tiles DefinitionsFactory that can be extended by activated plugins.
 */
public class ExtendableDefinitionsFactory implements DefinitionsFactory {

  private static final long serialVersionUID = 1L;

  public static final Log LOG = LogFactory
      .getLog(ExtendableDefinitionsFactory.class);

  DefinitionsFactoryConfig config;

  protected transient XmlParser xmlParser = new XmlParser();

  Map definitions;

  private ServletContext servletContext;

  HashMap locales = new HashMap();

  public ComponentDefinition getDefinition(String name, ServletRequest request,
      ServletContext servletContext) throws NoSuchDefinitionException,
      DefinitionsFactoryException {
    return (ComponentDefinition) definitions.get(name);
  }

  public void putDefinition(ComponentDefinition definition) {
    LOG.info("putting definition:" + definition.getName());
    definitions.put(definition.getName(), definition);
    try {
      definition.getOrCreateController();
    } catch (Exception e) {
      e.printStackTrace(LogUtil.getDebugStream(LOG));
    }
  }

  protected XmlDefinitionsSet getDefinitions() {

    LOG.debug("getDefinitions()");

    XmlDefinitionsSet definitions = new XmlDefinitionsSet();
    //
    // core definitions
    //
    String configFiles = config.getDefinitionConfigFiles();
    LOG.info("config files:" + configFiles);
    String[] files = configFiles.split(",");
    for (int i = 0; i < files.length; i++) {
      LOG.info("Trying to load " + files[i]);
      InputStream input = servletContext.getResourceAsStream(files[i]);

      LOG.info("Stream: " + input);

      if (input != null) {
        parseXMLDefinitionSet(input, definitions, files[i], "nutch-core");
      } else {
        LOG.info("Cannot find static " + files[i]);
      }
    }

    //
    // plugged implementations can override defaults if they wish
    //
    ExtensionPoint point = ServletContextServiceLocator.getInstance(
        servletContext).getPluginRepository().getExtensionPoint(
        UIExtensionPoint.X_POINT_ID);

    if (point != null) {

      Extension[] extensions = point.getExtensions();

      LOG.info("There are " + extensions.length
          + " extensions available for UI");

      for (int i = 0; i < extensions.length; i++) {
        LOG.info("Adding definitions from "
            + extensions[i].getDescriptor().getName());
        Extension extension = extensions[i];
        addToSet(definitions, extension);
      }
    } else {
      LOG.info("Cannot find extension point '" + UIExtensionPoint.X_POINT_ID
          + "'");
    }

    try {
      definitions.resolveInheritances();
    } catch (NoSuchDefinitionException e) {
      LOG.info("Error resolving:" + e);
    }
    return definitions;
  }

  protected void addToSet(XmlDefinitionsSet definitions, Extension extension) {

    InputStream is = extension.getDescriptor().getClassLoader()
        .getResourceAsStream("tiles-defs.xml");
    if (is == null) {
      LOG.info("Plugin " + extension.getId()
          + " did not contain tiles-defs.xml");
      return;
    }
    parseXMLDefinitionSet(is, definitions, "Plugin " + extension.getId()
        + " : tiles-defs.xml", extension.getId());
    try {
      is.close();
    } catch (Exception e) {
      e.printStackTrace(LogUtil.getDebugStream(LOG));
    }
  }

  protected void parseXMLDefinitionSet(InputStream input,
      XmlDefinitionsSet definitions, String info, String pluginid) {
    XmlDefinitionsSet newSet = new XmlDefinitionsSet();

    try {
      xmlParser.parse(input, newSet);
    } catch (IOException e) {
      LOG.info("IOException (" + e.getMessage() + ") parsing definitions "
          + info);
      e.printStackTrace();
    } catch (SAXException e) {
      LOG.info("SAXException (" + e.getMessage() + ") parsing definitions "
          + info);
      e.printStackTrace();
    }

    // LOG.info("Definitions:" + definitions.getDefinitions().size() + " : "
    // + definitions.toString());

    copySet(definitions, newSet);

  }

  private void copySet(XmlDefinitionsSet definitions2, XmlDefinitionsSet newSet) {

    Iterator iterator = newSet.getDefinitions().keySet().iterator();

    while (iterator.hasNext()) {
      String key = (String) iterator.next();
      LOG.info("adding: " + key);
      XmlDefinition value = newSet.getDefinition(key);
      definitions2.putDefinition(value);
    }
  }

  public void init(DefinitionsFactoryConfig config,
      ServletContext servletContext) throws DefinitionsFactoryException {
    this.config = config;

    this.servletContext = servletContext;

    xmlParser.setValidating(config.getParserValidate());
    XmlDefinitionsSet definitions = getDefinitions();
    ClassLoader current = Thread.currentThread().getContextClassLoader();

    PluginResourceLoader loader = ServletContextServiceLocator.getInstance(
        servletContext).getPluginResourceLoader(current);

    Thread.currentThread().setContextClassLoader(loader);
    initDefinitions(definitions);
    Thread.currentThread().setContextClassLoader(current);

    this.definitions = definitions.getDefinitions();
  }

  private void initDefinitions(XmlDefinitionsSet definitions) {
    Iterator i = definitions.getDefinitions().keySet().iterator();

    while (i.hasNext()) {
      String key = (String) i.next();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initializing controller: " + key);
      }
      XmlDefinition d = definitions.getDefinition(key);
      try {
        Controller controller = d.getOrCreateController();
        if (controller != null) {
          // check if it is implementing Startable, if so execute lifecycle
          // method
          if (controller instanceof Startable) {
            ((Startable) controller).start(servletContext);
          }
        }
      } catch (Exception e) {
        e.printStackTrace(LogUtil.getDebugStream(LOG));
      }
    }
  }

  public void destroy() {
    LOG.info("destroy()");
  }

  public void setConfig(DefinitionsFactoryConfig config,
      ServletContext servletContext) throws DefinitionsFactoryException {
    this.config = config;
    this.servletContext = servletContext;
  }

  public DefinitionsFactoryConfig getConfig() {
    return config;
  }
}

/*
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.LogFormatter;

import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.webapp.common.PluginResourceLoader;
import org.apache.nutch.webapp.common.ServletContextServiceLocator;
import org.apache.nutch.webapp.extension.UIExtensionPoint;
import org.apache.struts.tiles.ComponentDefinition;
import org.apache.struts.tiles.DefinitionsFactory;
import org.apache.struts.tiles.DefinitionsFactoryConfig;
import org.apache.struts.tiles.DefinitionsFactoryException;
import org.apache.struts.tiles.NoSuchDefinitionException;
import org.apache.struts.tiles.xmlDefinition.XmlDefinition;
import org.apache.struts.tiles.xmlDefinition.XmlDefinitionsSet;
import org.apache.struts.tiles.xmlDefinition.XmlParser;

import org.xml.sax.SAXException;

/**
 * Tiles DefinitionsFactory that can be extended by activated plugins
 */
public class ExtendableDefinitionsFactory implements DefinitionsFactory {

  private static final long serialVersionUID = 1L;

  public static Logger LOG = LogFormatter
      .getLogger(ExtendableDefinitionsFactory.class.getName());

  DefinitionsFactoryConfig config;

  protected transient XmlParser xmlParser = new XmlParser();

  Map definitions;

  private ServletContext servletContext;

  HashMap locales = new HashMap();

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.struts.tiles.DefinitionsFactory#getDefinition(java.lang.String,
   *      javax.servlet.ServletRequest, javax.servlet.ServletContext)
   */
  public ComponentDefinition getDefinition(String name, ServletRequest request,
      ServletContext servletContext) throws NoSuchDefinitionException,
      DefinitionsFactoryException {
    return (ComponentDefinition) definitions.get(name);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.struts.tiles.xmlDefinition.DefinitionsFactory#putDefinition(org.apache.struts.tiles.ComponentDefinition)
   */
  public void putDefinition(ComponentDefinition definition) {
    LOG.info("putting definition:" + definition.getName());
    definitions.put(definition.getName(), definition);
    try {
      definition.getOrCreateController();
    } catch (Exception e) {
      e.printStackTrace(System.out);
    }
    LOG.fine("Restoring ClassLoader.");

  }

  protected XmlDefinitionsSet getDefinitions() {

    XmlDefinitionsSet definitions = new XmlDefinitionsSet();
    //
    // global definitions
    //
    String configFiles = config.getDefinitionConfigFiles();
    LOG.info("config files:" + configFiles);
    String[] files = configFiles.split(",");
    for (int i = 0; i < files.length; i++) {
      LOG.info("Trying to load " + files[i]);
      InputStream input = servletContext.getResourceAsStream(files[i]);

      if (input != null) {
        parseXMLDefinitionSet(input, definitions, files[i]);
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
      LOG.info("Cannot find extension point '" + UIExtensionPoint.X_POINT_ID + "'");
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
        + " : tiles-defs.xml");
    try {
      is.close();
    } catch (Exception e) {
      e.printStackTrace(System.out);
    }
  }

  protected void parseXMLDefinitionSet(InputStream input,
      XmlDefinitionsSet definitions, String info) {
    try {
      xmlParser.parse(input, definitions);
    } catch (IOException e) {
      LOG.info("IOException (" + e.getMessage() + ") parsing definitions "
          + info);
      e.printStackTrace();
    } catch (SAXException e) {
      LOG.info("SAXException (" + e.getMessage() + ") parsing definitions "
          + info);
      e.printStackTrace();
    }

    LOG.info("Definitions:" + definitions.getDefinitions().size() + " : "
        + definitions.toString());
  }

  protected Configuration getNutchConfig(ServletContext context) {
    return ServletContextServiceLocator.getInstance(context).getConfiguration();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.struts.tiles.DefinitionsFactory#init(org.apache.struts.tiles.DefinitionsFactoryConfig,
   *      javax.servlet.ServletContext)
   */
  public void init(DefinitionsFactoryConfig config,
      ServletContext servletContext) throws DefinitionsFactoryException {
    this.config = config;

    this.servletContext = servletContext;

    xmlParser.setValidating(config.getParserValidate());
    XmlDefinitionsSet definitions = getDefinitions();
    ClassLoader current = Thread.currentThread().getContextClassLoader();

    PluginResourceLoader loader = ServletContextServiceLocator.getInstance(
        servletContext).getPluginResourceLoader(current);

    //TODO: fix this!!!
    Thread.currentThread().setContextClassLoader(loader);
    initDefinitions(definitions);
    Thread.currentThread().setContextClassLoader(current);

    this.definitions = definitions.getDefinitions();

  }

  private void initDefinitions(XmlDefinitionsSet definitions) {
    Iterator i = definitions.getDefinitions().keySet().iterator();

    while (i.hasNext()) {
      String key = (String) i.next();
      LOG.fine("Initializing controller: " + key);
      XmlDefinition d = definitions.getDefinition(key);
      try {
        d.getOrCreateController();
      } catch (Exception e) {
        e.printStackTrace(System.out);
      }
    }
    LOG.fine("Restoring ClassLoader.");
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.struts.tiles.DefinitionsFactory#destroy()
   */
  public void destroy() {
    LOG.info("destroy()");
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.struts.tiles.DefinitionsFactory#setConfig(org.apache.struts.tiles.DefinitionsFactoryConfig,
   *      javax.servlet.ServletContext)
   */
  public void setConfig(DefinitionsFactoryConfig config,
      ServletContext servletContext) throws DefinitionsFactoryException {
    this.config = config;
    this.servletContext = servletContext;
  }

  public DefinitionsFactoryConfig getConfig() {
    return config;
  }

}

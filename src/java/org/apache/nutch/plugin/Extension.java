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
package org.apache.nutch.plugin;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;

/**
 * An <code>Extension</code> is a kind of listener descriptor that will be
 * installed on a concrete <code>ExtensionPoint</code> that acts as kind of
 * Publisher.
 */
public class Extension {
  private PluginDescriptor fDescriptor;
  private String fId;
  private String fTargetPoint;
  private String fClazz;
  private HashMap<String, String> fAttributes;
  private Configuration conf;
  private PluginRepository pluginRepository;

  /**
   * @param pDescriptor
   *          a plugin descriptor
   * @param pExtensionPoint
   *          an extension porin
   * @param pId
   *          an unique id of the plugin
   */
  public Extension(PluginDescriptor pDescriptor, String pExtensionPoint,
      String pId, String pExtensionClass, Configuration conf,
      PluginRepository pluginRepository) {
    fAttributes = new HashMap<String, String>();
    setDescriptor(pDescriptor);
    setExtensionPoint(pExtensionPoint);
    setId(pId);
    setClazz(pExtensionClass);
    this.conf = conf;
    this.pluginRepository = pluginRepository;
  }

  /**
   * @param point
   */
  private void setExtensionPoint(String point) {
    fTargetPoint = point;
  }

  /**
   * Returns a attribute value, that is setuped in the manifest file and is
   * definied by the extension point xml schema.
   * 
   * @param pKey
   *          a key
   * @return String a value
   */
  public String getAttribute(String pKey) {
    return fAttributes.get(pKey);
  }

  /**
   * Returns the full class name of the extension point implementation
   * 
   * @return String
   */
  public String getClazz() {
    return fClazz;
  }

  /**
   * Return the unique id of the extension.
   * 
   * @return String
   */
  public String getId() {
    return fId;
  }

  /**
   * Adds a attribute and is only used until model creation at plugin system
   * start up.
   * 
   * @param pKey a key
   * @param pValue a value
   */
  public void addAttribute(String pKey, String pValue) {
    fAttributes.put(pKey, pValue);
  }

  /**
   * Sets the Class that implement the concret extension and is only used until
   * model creation at system start up.
   * 
   * @param extensionClazz The extensionClasname to set
   */
  public void setClazz(String extensionClazz) {
    fClazz = extensionClazz;
  }

  /**
   * Sets the unique extension Id and is only used until model creation at
   * system start up.
   * 
   * @param extensionID The extensionID to set
   */
  public void setId(String extensionID) {
    fId = extensionID;
  }

  /**
   * Returns the Id of the extension point, that is implemented by this
   * extension.
   */
  public String getTargetPoint() {
    return fTargetPoint;
  }

  /**
   * Return an instance of the extension implementatio. Before we create a
   * extension instance we startup the plugin if it is not already done. The
   * plugin instance and the extension instance use the same
   * <code>PluginClassLoader</code>. Each Plugin use its own classloader. The
   * PluginClassLoader knows only own <i>Plugin runtime libraries </i> setuped
   * in the plugin manifest file and exported libraries of the depenedend
   * plugins.
   * 
   * @return Object An instance of the extension implementation
   */
  public Object getExtensionInstance() throws PluginRuntimeException {
    // Must synchronize here to make sure creation and initialization
    // of a plugin instance and it extension instance are done by
    // one and only one thread.
    // The same is in PluginRepository.getPluginInstance().
    // Suggested by Stefan Groschupf <sg@media-style.com>
    synchronized (getId()) {
      try {
        PluginClassLoader loader = fDescriptor.getClassLoader();
        Class<?> extensionClazz = loader.loadClass(getClazz());
        // lazy loading of Plugin in case there is no instance of the plugin
        // already.
        this.pluginRepository.getPluginInstance(getDescriptor());
        Object object = extensionClazz.newInstance();
        if (object instanceof Configurable) {
          ((Configurable) object).setConf(this.conf);
        }
        return object;
      } catch (ClassNotFoundException e) {
        throw new PluginRuntimeException(e);
      } catch (InstantiationException e) {
        throw new PluginRuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new PluginRuntimeException(e);
      }
    }
  }

  /**
   * return the plugin descriptor.
   * 
   * @return PluginDescriptor
   */
  public PluginDescriptor getDescriptor() {
    return fDescriptor;
  }

  /**
   * Sets the plugin descriptor and is only used until model creation at system
   * start up.
   * 
   * @param pDescriptor
   */
  public void setDescriptor(PluginDescriptor pDescriptor) {
    fDescriptor = pDescriptor;
  }
}

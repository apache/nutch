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

import java.lang.reflect.InvocationTargetException;
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

  /**
   * @param pDescriptor
   *          a plugin descriptor
   * @param pExtensionPoint
   *          an extension point
   * @param pId
   *          an unique id of the plugin
   * @param pExtensionClass the class extending the extension point
   * @param conf a populated {@link org.apache.hadoop.conf.Configuration}
   * @param pluginRepository a {@link PluginRepository} containing 
   * all plugin artifacts
   */
  public Extension(PluginDescriptor pDescriptor, String pExtensionPoint,
      String pId, String pExtensionClass, Configuration conf,
      PluginRepository pluginRepository) {
    fAttributes = new HashMap<>();
    setDescriptor(pDescriptor);
    setExtensionPoint(pExtensionPoint);
    setId(pId);
    setClazz(pExtensionClass);
    this.conf = conf;
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
   * @param pKey
   *          a key
   * @param pValue
   *          a value
   */
  public void addAttribute(String pKey, String pValue) {
    fAttributes.put(pKey, pValue);
  }

  /**
   * Sets the Class that implement the concret extension and is only used until
   * model creation at system start up.
   * 
   * @param extensionClazz
   *          The extensionClasname to set
   */
  public void setClazz(String extensionClazz) {
    fClazz = extensionClazz;
  }

  /**
   * Sets the unique extension Id and is only used until model creation at
   * system start up.
   * 
   * @param extensionID
   *          The extensionID to set
   */
  public void setId(String extensionID) {
    fId = extensionID;
  }

  /**
   * Get target point
   * @return the Id of the extension point, that is implemented by this
   * extension.
   */
  public String getTargetPoint() {
    return fTargetPoint;
  }

  /**
   * Return an instance of the extension implementation. Before we create a
   * extension instance we startup the plugin if it is not already done. The
   * plugin instance and the extension instance use the same
   * {@link org.apache.nutch.plugin.PluginClassLoader}. Each Plugin uses its own
   * classloader. The {@link org.apache.nutch.plugin.PluginClassLoader} knows
   * only its own <i>plugin runtime libraries</i> defined in the
   * <code>plugin.xml</code> manifest file and exported libraries of the
   * dependent plugins.
   * 
   * @return Object An instance of the extension implementation
   * @throws PluginRuntimeException
   *           if there is a fatal runtime error
   */
  public Object getExtensionInstance() throws PluginRuntimeException {
    // Must synchronize here to make sure creation and initialization
    // of a plugin instance and it extension instance are done by
    // one and only one thread.
    // The same is in PluginRepository.getPluginInstance().
    // Suggested by Stefan Groschupf <sg@media-style.com>
    synchronized (getId()) {
      try {
        PluginRepository pluginRepository = PluginRepository.get(conf);
        Class<?> extensionClazz = pluginRepository.getCachedClass(fDescriptor,
            getClazz());
        // lazy loading of Plugin in case there is no instance of the plugin
        // already.
        pluginRepository.getPluginInstance(getDescriptor());
        Object object = null;
        try {
          object = extensionClazz.getConstructor().newInstance();
        } catch (IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
          e.printStackTrace();
        }
        if (object != null && object instanceof Configurable) {
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
   * Get the plugin descriptor.
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
   * @param pDescriptor a instantiated {@link org.apache.nutch.plugin.PluginDescriptor}
   */
  public void setDescriptor(PluginDescriptor pDescriptor) {
    fDescriptor = pDescriptor;
  }

  public String toString() {
    return getId() + ", " + getClazz() + ", " + getTargetPoint();
  }
}

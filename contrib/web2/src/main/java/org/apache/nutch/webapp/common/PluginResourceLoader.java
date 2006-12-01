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
package org.apache.nutch.webapp.common;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginClassLoader;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.webapp.extension.UIExtensionPoint;

/**
 * PluginResourceLoader is capable of loading plugged in resources.
 */
public class PluginResourceLoader extends ClassLoader {

  public class ThrowAwayClassLoader extends ClassLoader {

    private ClassLoader one;

    private ClassLoader two;

    public ThrowAwayClassLoader(ClassLoader two, ClassLoader one) {
      this.one = one;
      this.two = two;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.ClassLoader#getResource(java.lang.String)
     */
    public URL getResource(String name) {
      URL retVal = one.getResource(name);
      if (retVal == null)
        retVal = two.getResource(name);
      return retVal;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.ClassLoader#getResourceAsStream(java.lang.String)
     */
    public InputStream getResourceAsStream(String name) {
      InputStream retVal = one.getResourceAsStream(name);
      if (retVal == null)
        retVal = two.getResourceAsStream(name);
      return retVal;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.ClassLoader#getResources(java.lang.String)
     */
    public Enumeration getResources(String name) throws IOException {
      Enumeration retVal = one.getResources(name);
      if (retVal == null)
        retVal = two.getResources(name);
      return retVal;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.ClassLoader#loadClass(java.lang.String)
     */
    public Class loadClass(String name) throws ClassNotFoundException {
      if(LOG.isDebugEnabled()){
        LOG.debug("loading class " + name + " from " + one);
      }
      try {
        return one.loadClass(name);
      } catch (ClassNotFoundException e) {
      }
      if(LOG.isDebugEnabled()){
          LOG.debug("loading class " + name + " from " + two);
      }
      try {
        return two.loadClass(name);
      } catch (ClassNotFoundException e) {
      }
      if(LOG.isDebugEnabled()){
        LOG.info("could not find class, must throw Exception");
      }
      throw new ClassNotFoundException(name);
    }
  }

  ClassLoader wrapped;

  ArrayList classloaders = new ArrayList();

  public static Log LOG = LogFactory.getLog(PluginResourceLoader.class);

  public static PluginResourceLoader getInstance(ServiceLocator locator,
      ClassLoader parent) {
    PluginResourceLoader loader = (PluginResourceLoader) locator
        .getConfiguration().getObject(PluginResourceLoader.class.getName());

    if (loader == null) {
      LOG.info("Created new nutch loader with parent loader:" + parent);
      loader = new PluginResourceLoader(locator, parent);
      locator.getConfiguration().setObject(
          PluginResourceLoader.class.getName(), loader);
    }
    return loader;
  }

  private PluginResourceLoader(ServiceLocator locator, ClassLoader parent) {
    init(locator, parent);
  }

  protected void init(ServiceLocator locator, ClassLoader parent) {

    ArrayList seen = new ArrayList();

    ArrayList paths = new ArrayList();

    if (LOG.isDebugEnabled()) {
      LOG.debug("PluginResourceLoader : dynamically setting jars based "
          + "on plugins implementing UIExtensionPoint.");
    }

    ExtensionPoint point = locator.getPluginRepository().getExtensionPoint(
        UIExtensionPoint.X_POINT_ID);

    if (point == null) {
      LOG.info("Can't find extension point '" + UIExtensionPoint.X_POINT_ID
          + "'");
      classloaders.add(parent);
      return;
    }

    Extension[] extensions = point.getExtensions();

    for (int i = 0; i < extensions.length; i++) {
      Extension extension = extensions[i];

      PluginClassLoader loader = extension.getDescriptor().getClassLoader();

      // classloaders.add(loader);

      URL[] urls = loader.getURLs();

      for (int k = 0; k < urls.length; k++) {
        URL url = urls[k];
        if (!seen.contains(url)) {
          paths.add(url);
          LOG.debug("Adding to classpath:" + url);
        }
        seen.add(url);
      }
    }

    URL[] urls = (URL[]) paths.toArray(new URL[paths.size()]);
    classloaders.add(new URLClassLoader(urls, parent));

  }

  public URL getResource(String name) {
    Iterator i = classloaders.iterator();

    while (i.hasNext()) {
      ClassLoader loader = (ClassLoader) i.next();
      URL retVal = loader.getResource(name);
      if (retVal != null) {
        return retVal;
      }
    }
    return null;
  }

  public InputStream getResourceAsStream(String name) {
    Iterator i = classloaders.iterator();

    while (i.hasNext()) {
      ClassLoader loader = (ClassLoader) i.next();
      InputStream retVal = loader.getResourceAsStream(name);
      if (retVal != null) {
        return retVal;
      }
    }
    return null;
  }

  public Enumeration getResources(String name) throws IOException {
    Iterator i = classloaders.iterator();

    while (i.hasNext()) {
      ClassLoader loader = (ClassLoader) i.next();
      Enumeration retVal = loader.getResources(name);
      if (retVal != null) {
        return retVal;
      }
    }
    return null;
  }

  public Class loadClass(String name) throws ClassNotFoundException {

    try {
      // LOG.info("CUSTOM_LOADER->load");
      Iterator i = classloaders.iterator();
      Class retVal = null;
      while (i.hasNext()) {

        ClassLoader loader = (ClassLoader) i.next();
        // LOG.info("trying to load " + name + " from:" + loader);
        try {
          retVal = loader.loadClass(name);
        } catch (Exception e) {
          LOG.info("Exception in loader " + e);
        }
        if (retVal != null) {
          // LOG.info("CUSTOM_LOADER->found");
          return retVal;
        }
      }
      // LOG.info("CUSTOM_LOADER_not found");
    } catch (Exception e) {
      LOG.info("Exception in loader " + e);
      e.printStackTrace(LogUtil.getInfoStream(LOG));
    }
    throw new ClassNotFoundException(name);
  }
}

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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

/**
 * The <code>PluginClassLoader</code> is a child-first classloader that only
 * contains classes of the runtime libraries setuped in the plugin manifest file
 * and exported libraries of plugins that are required plugins. Libraries can be
 * exported or not. Not exported libraries are only used in the plugin own
 * <code>PluginClassLoader</code>. Exported libraries are available for
 * <code>PluginClassLoader</code> of plugins that depends on these plugins.
 * 
 */
public class PluginClassLoader extends URLClassLoader {

  private URL[] urls;
  private ClassLoader parent;
  private ClassLoader system = getSystemClassLoader();

  /**
   * Construtor
   * 
   * @param urls
   *          Array of urls with own libraries and all exported libraries of
   *          plugins that are required to this plugin
   * @param parent
   */
  public PluginClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);

    this.urls = urls;
    this.parent = parent;
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {

    // First, check if the class has already been loaded
    Class<?> c = findLoadedClass(name);

    if (c == null) {
      try {
        // checking local
        c = findClass(name);
      } catch (ClassNotFoundException | SecurityException e) {
        c = loadClassFromParent(name, resolve);
      }
    }

    if (resolve) {
      resolveClass(c);
    }

    return c;
  }

  private Class<?> loadClassFromParent(String name, boolean resolve)
      throws ClassNotFoundException {
    // checking parent
    // This call to loadClass may eventually call findClass
    // again, in case the parent doesn't find anything.
    Class<?> c;
    try {
      c = super.loadClass(name, resolve);
    } catch (ClassNotFoundException e) {
      c = loadClassFromSystem(name);
    } catch (SecurityException e) {
      c = loadClassFromSystem(name);
    }
    return c;
  }

  private Class<?> loadClassFromSystem(String name)
      throws ClassNotFoundException {
    Class<?> c = null;
    if (system != null) {
      // checking system: jvm classes, endorsed, cmd classpath,
      c = system.loadClass(name);
    }
    return c;
  }

  @Override
  public URL getResource(String name) {
    URL url = findResource(name);
    if (url == null)
      url = super.getResource(name);

    if (url == null && system != null)
      url = system.getResource(name);

    return url;
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    /**
     * Similar to super, but local resources are enumerated before parent
     * resources
     */
    Enumeration<URL> systemUrls = null;
    if (system != null) {
      systemUrls = system.getResources(name);
    }

    Enumeration<URL> localUrls = findResources(name);
    Enumeration<URL> parentUrls = null;
    if (getParent() != null) {
      parentUrls = getParent().getResources(name);
    }

    final List<URL> urls = new ArrayList<URL>();
    if (localUrls != null) {
      while (localUrls.hasMoreElements()) {
        URL local = localUrls.nextElement();
        urls.add(local);
      }
    }

    if (systemUrls != null) {
      while (systemUrls.hasMoreElements()) {
        urls.add(systemUrls.nextElement());
      }
    }

    if (parentUrls != null) {
      while (parentUrls.hasMoreElements()) {
        urls.add(parentUrls.nextElement());
      }
    }

    return new Enumeration<URL>() {
      Iterator<URL> iter = urls.iterator();

      public boolean hasMoreElements() {
        return iter.hasNext();
      }

      public URL nextElement() {
        return iter.next();
      }
    };
  }

  public InputStream getResourceAsStream(String name) {
    URL url = getResource(name);
    try {
      return url != null ? url.openStream() : null;
    } catch (IOException e) {
    }
    return null;
  }

  @Override
  public int hashCode() {
    final int PRIME = 31;
    int result = 1;
    result = PRIME * result + ((parent == null) ? 0 : parent.hashCode());
    result = PRIME * result + Arrays.hashCode(urls);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final PluginClassLoader other = (PluginClassLoader) obj;
    if (parent == null) {
      if (other.parent != null)
        return false;
    } else if (!parent.equals(other.parent))
      return false;
    if (!Arrays.equals(urls, other.urls))
      return false;
    return true;
  }
}

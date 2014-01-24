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

/**
 * The <code>PluginClassLoader</code> contains only classes of the runtime
 * libraries setuped in the plugin manifest file and exported libraries of
 * plugins that are required pluguin. Libraries can be exported or not. Not
 * exported libraries are only used in the plugin own
 * <code>PluginClassLoader</code>. Exported libraries are available for
 * <code>PluginClassLoader</code> of plugins that depends on these plugins.
 * 
 * @author joa23
 */
public class PluginClassLoader extends URLClassLoader {

  private URL[] urls;
  private ClassLoader parent;

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

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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * The <code>PluginDescriptor</code> provide access to all meta information of
 * a nutch-plugin, as well to the internationalizable resources and the plugin
 * own classloader. There are meta information about <code>Plugin</code>,
 * <code>ExtensionPoint</code> and <code>Extension</code>. To provide
 * access to the meta data of a plugin via a descriptor allow a lazy loading
 * mechanism.
 * 
 * @author joa23
 */
public class PluginDescriptor {
  private String fPluginPath;
  private String fPluginClass = Plugin.class.getName();
  private String fPluginId;
  private String fVersion;
  private String fName;
  private String fProviderName;
  private HashMap fMessages = new HashMap();
  private ArrayList<ExtensionPoint> fExtensionPoints = new ArrayList<ExtensionPoint>();
  private ArrayList<String> fDependencies = new ArrayList<String>();
  private ArrayList<URL> fExportedLibs = new ArrayList<URL>();
  private ArrayList<URL> fNotExportedLibs = new ArrayList<URL>();
  private ArrayList<Extension> fExtensions = new ArrayList<Extension>();
  private PluginClassLoader fClassLoader;
  public static final Log LOG = LogFactory.getLog(PluginDescriptor.class);
  private Configuration fConf;

  /**
   * Constructor
   * 
   * @param pId
   * @param pVersion
   * @param pName
   * @param pProviderName
   * @param pPluginclazz
   * @param pPath
   */
  public PluginDescriptor(String pId, String pVersion, String pName,
      String pProviderName, String pPluginclazz, String pPath,
      Configuration conf) {
    setPath(pPath);
    setPluginId(pId);
    setVersion(pVersion);
    setName(pName);
    setProvidername(pProviderName);

    if (pPluginclazz != null)
      setPluginClass(pPluginclazz);

    this.fConf = conf;
  }

  /**
   * @param pPath
   */
  private void setPath(String pPath) {
    fPluginPath = pPath;
  }

  /**
   * Returns the name of the plugin.
   * 
   * @return String
   */
  public String getName() {
    return fName;
  }

  /**
   * @param providerName
   */
  private void setProvidername(String providerName) {
    fProviderName = providerName;
  }

  /**
   * @param name
   */
  private void setName(String name) {
    fName = name;
  }

  /**
   * @param version
   */
  private void setVersion(String version) {
    fVersion = version;
  }

  /**
   * Returns the fully qualified name of the class which implements the abstarct
   * <code>Plugin</code> class.
   * 
   * @return the name of this plug-in's runtime class or <code>null</code>.
   */
  public String getPluginClass() {
    return fPluginClass;
  }

  /**
   * Returns the unique identifier of the plug-in or <code>null</code>.
   * 
   * @return String
   */
  public String getPluginId() {
    return fPluginId;
  }

  /**
   * Returns an array of extensions.
   * 
   * @return Exception[]
   */
  public Extension[] getExtensions() {
    return fExtensions.toArray(new Extension[fExtensions.size()]);
  }

  /**
   * Adds a extension.
   * 
   * @param pExtension
   */
  public void addExtension(Extension pExtension) {
    fExtensions.add(pExtension);
  }

  /**
   * Sets the pluginClass.
   * 
   * @param pluginClass
   *          The pluginClass to set
   */
  private void setPluginClass(String pluginClass) {
    fPluginClass = pluginClass;
  }

  /**
   * Sets the plugin Id.
   * 
   * @param pluginId
   *          The pluginId to set
   */
  private void setPluginId(String pluginId) {
    fPluginId = pluginId;
  }

  /**
   * Adds a extension point.
   * 
   * @param extensionPoint
   */
  public void addExtensionPoint(ExtensionPoint extensionPoint) {
    fExtensionPoints.add(extensionPoint);
  }

  /**
   * Returns a array of extension points.
   * 
   * @return ExtensionPoint[]
   */
  public ExtensionPoint[] getExtenstionPoints() {
    return fExtensionPoints
        .toArray(new ExtensionPoint[fExtensionPoints.size()]);
  }

  /**
   * Returns a array of plugin ids.
   * 
   * @return String[]
   */
  public String[] getDependencies() {
    return fDependencies.toArray(new String[fDependencies.size()]);
  }

  /**
   * Adds a dependency
   * 
   * @param pId id of the dependent plugin
   */
  public void addDependency(String pId) {
    fDependencies.add(pId);
  }

  /**
   * Adds a exported library with a relative path to the plugin directory.
   * 
   * @param pLibPath
   */
  public void addExportedLibRelative(String pLibPath)
      throws MalformedURLException {
    URL url = new File(getPluginPath() + File.separator + pLibPath).toURL();
    fExportedLibs.add(url);
  }

  /**
   * Returns the directory path of the plugin.
   * 
   * @return String
   */
  public String getPluginPath() {
    return fPluginPath;
  }

  /**
   * Returns a array exported librareis as URLs
   * 
   * @return URL[]
   */
  public URL[] getExportedLibUrls() {
    return fExportedLibs.toArray(new URL[0]);
  }

  /**
   * Adds a not exported library with a plugin directory relative path.
   * 
   * @param pLibPath
   */
  public void addNotExportedLibRelative(String pLibPath)
      throws MalformedURLException {
    URL url = new File(getPluginPath() + File.separator + pLibPath).toURL();
    fNotExportedLibs.add(url);
  }

  /**
   * Returns a array of libraries as URLs that are not exported by the plugin.
   * 
   * @return URL[]
   */
  public URL[] getNotExportedLibUrls() {
    return fNotExportedLibs.toArray(new URL[fNotExportedLibs.size()]);
  }

  /**
   * Returns a cached classloader for a plugin. Until classloader creation all
   * needed libraries are collected. A classloader use as first the plugins own
   * libraries and add then all exported libraries of dependend plugins.
   * 
   * @return PluginClassLoader the classloader for the plugin
   */
  public PluginClassLoader getClassLoader() {
    if (fClassLoader != null)
      return fClassLoader;
    ArrayList<URL> arrayList = new ArrayList<URL>();
    arrayList.addAll(fExportedLibs);
    arrayList.addAll(fNotExportedLibs);
    arrayList.addAll(getDependencyLibs());
    File file = new File(getPluginPath());
    try {
      for (File file2 : file.listFiles()) {
        if (file2.getAbsolutePath().endsWith("properties"))
          arrayList.add(file2.getParentFile().toURL());
      }
    } catch (MalformedURLException e) {
      LOG.debug(getPluginId() + " " + e.toString());
    }
    URL[] urls = arrayList.toArray(new URL[arrayList.size()]);
    fClassLoader = new PluginClassLoader(urls, PluginDescriptor.class
        .getClassLoader());
    return fClassLoader;
  }

  /**
   * @return Collection
   */
  private ArrayList<URL> getDependencyLibs() {
    ArrayList<URL> list = new ArrayList<URL>();
    collectLibs(list, this);
    return list;
  }

  /**
   * @param pLibs
   * @param pDescriptor
   */
  private void collectLibs(ArrayList<URL> pLibs, PluginDescriptor pDescriptor) {

    for (String id : pDescriptor.getDependencies()) {
      PluginDescriptor descriptor = PluginRepository.get(fConf)
          .getPluginDescriptor(id);
      for (URL url: descriptor.getExportedLibUrls()) {
        pLibs.add(url);
      }
      collectLibs(pLibs, descriptor);
    }
  }

  /**
   * Returns a I18N'd resource string. The resource bundles could be stored in
   * root directory of a plugin in the well know i18n file name conventions.
   * 
   * @param pKey
   * @param pLocale
   * @return String
   * @throws IOException
   */
  public String getResourceString(String pKey, Locale pLocale)
      throws IOException {
    if (fMessages.containsKey(pLocale.toString())) {
      ResourceBundle bundle = (ResourceBundle) fMessages
          .get(pLocale.toString());
      try {
        return bundle.getString(pKey);
      } catch (MissingResourceException e) {
        return '!' + pKey + '!';
      }
    }
    try {
      ResourceBundle res = ResourceBundle.getBundle("messages", pLocale,
          getClassLoader());
      return res.getString(pKey);
    } catch (MissingResourceException x) {
      return '!' + pKey + '!';
    }
  }

  public String getProviderName() {
    return fProviderName;
  }

  public String getVersion() {
    return fVersion;
  }
}

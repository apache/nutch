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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.WeakHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

/**
 * The plugin repositority is a registry of all plugins.
 * 
 * At system boot up a repositority is builded by parsing the mainifest files of
 * all plugins. Plugins that require not existing other plugins are not
 * registed. For each plugin a plugin descriptor instance will be created. The
 * descriptor represents all meta information about a plugin. So a plugin
 * instance will be created later when it is required, this allow lazy plugin
 * loading.
 */
public class PluginRepository {
  private static final WeakHashMap<String, PluginRepository> CACHE = new WeakHashMap<String, PluginRepository>();

  private boolean auto;

  private List<PluginDescriptor> fRegisteredPlugins;

  private HashMap<String, ExtensionPoint> fExtensionPoints;

  private HashMap<String, Plugin> fActivatedPlugins;

  private Configuration conf;

  public static final Logger LOG = LoggerFactory.getLogger(PluginRepository.class);

  /**
   * @throws PluginRuntimeException
   * @see java.lang.Object#Object()
   */
  public PluginRepository(Configuration conf) throws RuntimeException {
    fActivatedPlugins = new HashMap<String, Plugin>();
    fExtensionPoints = new HashMap<String, ExtensionPoint>();
    this.conf = conf;
    this.auto = conf.getBoolean("plugin.auto-activation", true);
    String[] pluginFolders = conf.getStrings("plugin.folders");
    PluginManifestParser manifestParser = new PluginManifestParser(conf, this);
    Map<String, PluginDescriptor> allPlugins = manifestParser
        .parsePluginFolder(pluginFolders);
    Pattern excludes = Pattern.compile(conf.get("plugin.excludes", ""));
    Pattern includes = Pattern.compile(conf.get("plugin.includes", ""));
    Map<String, PluginDescriptor> filteredPlugins = filter(excludes, includes,
        allPlugins);
    fRegisteredPlugins = getDependencyCheckedPlugins(filteredPlugins,
        this.auto ? allPlugins : filteredPlugins);
    installExtensionPoints(fRegisteredPlugins);
    try {
      installExtensions(fRegisteredPlugins);
    } catch (PluginRuntimeException e) {
        LOG.error(e.toString());
      throw new RuntimeException(e.getMessage());
    }
    displayStatus();
  }

  /**
   * @return a cached instance of the plugin repository
   */
  public static synchronized PluginRepository get(Configuration conf) {
    String uuid = NutchConfiguration.getUUID(conf);
    if (uuid == null) {
      uuid = "nonNutchConf@" + conf.hashCode(); // fallback
    }
    PluginRepository result = CACHE.get(uuid);
    if (result == null) {
      result = new PluginRepository(conf);
      CACHE.put(uuid, result);
    }
    return result;
  }

  private void installExtensionPoints(List<PluginDescriptor> plugins) {
    if (plugins == null) {
      return;
    }

    for (PluginDescriptor plugin: plugins) {
      for(ExtensionPoint point:plugin.getExtenstionPoints()) {
        String xpId = point.getId();
        LOG.debug("Adding extension point " + xpId);
        fExtensionPoints.put(xpId, point);
      }
    }
  }

  /**
   * @param pRegisteredPlugins
   */
  private void installExtensions(List<PluginDescriptor> pRegisteredPlugins)
      throws PluginRuntimeException {

    for (PluginDescriptor descriptor : pRegisteredPlugins) {
      for(Extension extension:descriptor.getExtensions()) {
        String xpId = extension.getTargetPoint();
        ExtensionPoint point = getExtensionPoint(xpId);
        if (point == null) {
          throw new PluginRuntimeException("Plugin ("
              + descriptor.getPluginId() + "), " + "extension point: " + xpId
              + " does not exist.");
        }
        point.addExtension(extension);
      }
    }
  }

  private void getPluginCheckedDependencies(PluginDescriptor plugin,
      Map<String, PluginDescriptor> plugins,
      Map<String, PluginDescriptor> dependencies,
      Map<String, PluginDescriptor> branch) throws MissingDependencyException,
      CircularDependencyException {

    if (dependencies == null) {
      dependencies = new HashMap<String, PluginDescriptor>();
    }
    if (branch == null) {
      branch = new HashMap<String, PluginDescriptor>();
    }
    branch.put(plugin.getPluginId(), plugin);

    // Otherwise, checks each dependency
    for(String id:plugin.getDependencies()) {
      PluginDescriptor dependency = plugins.get(id);
      if (dependency == null) {
        throw new MissingDependencyException("Missing dependency " + id
            + " for plugin " + plugin.getPluginId());
      }
      if (branch.containsKey(id)) {
        throw new CircularDependencyException("Circular dependency detected "
            + id + " for plugin " + plugin.getPluginId());
      }
      dependencies.put(id, dependency);
      getPluginCheckedDependencies(plugins.get(id), plugins, dependencies,
          branch);
    }

    branch.remove(plugin.getPluginId());
  }

  private Map<String, PluginDescriptor> getPluginCheckedDependencies(
      PluginDescriptor plugin, Map<String, PluginDescriptor> plugins)
      throws MissingDependencyException, CircularDependencyException {
    Map<String, PluginDescriptor> dependencies = new HashMap<String, PluginDescriptor>();
    Map<String, PluginDescriptor> branch = new HashMap<String, PluginDescriptor>();
    getPluginCheckedDependencies(plugin, plugins, dependencies, branch);
    return dependencies;
  }

  /**
   * @param filtered
   *          is the list of plugin filtred
   * @param all
   *          is the list of all plugins found.
   * @return List
   */
  private List<PluginDescriptor> getDependencyCheckedPlugins(
      Map<String, PluginDescriptor> filtered, Map<String, PluginDescriptor> all) {
    if (filtered == null) {
      return null;
    }
    Map<String, PluginDescriptor> checked = new HashMap<String, PluginDescriptor>();

    for (PluginDescriptor plugin : filtered.values()) {
      try {
        checked.putAll(getPluginCheckedDependencies(plugin, all));
        checked.put(plugin.getPluginId(), plugin);
      } catch (MissingDependencyException mde) {
        // Logger exception and ignore plugin
        LOG.warn(mde.getMessage());
      } catch (CircularDependencyException cde) {
        // Simply ignore this plugin
        LOG.warn(cde.getMessage());
      }
    }
    return new ArrayList<PluginDescriptor>(checked.values());
  }

  /**
   * Returns all registed plugin descriptors.
   * 
   * @return PluginDescriptor[]
   */
  public PluginDescriptor[] getPluginDescriptors() {
    return fRegisteredPlugins.toArray(new PluginDescriptor[fRegisteredPlugins
        .size()]);
  }

  /**
   * Returns the descriptor of one plugin identified by a plugin id.
   * 
   * @param pPluginId
   * @return PluginDescriptor
   */
  public PluginDescriptor getPluginDescriptor(String pPluginId) {

    for (PluginDescriptor descriptor : fRegisteredPlugins) {
      if (descriptor.getPluginId().equals(pPluginId))
        return descriptor;
    }
    return null;
  }

  /**
   * Returns a extension point indentified by a extension point id.
   * 
   * @param pXpId
   * @return a extentsion point
   */
  public ExtensionPoint getExtensionPoint(String pXpId) {
    return this.fExtensionPoints.get(pXpId);
  }

  /**
   * Returns a instance of a plugin. Plugin instances are cached. So a plugin
   * exist only as one instance. This allow a central management of plugin own
   * resources.
   * 
   * After creating the plugin instance the startUp() method is invoked. The
   * plugin use a own classloader that is used as well by all instance of
   * extensions of the same plugin. This class loader use all exported libraries
   * from the dependend plugins and all plugin libraries.
   * 
   * @param pDescriptor
   * @return Plugin
   * @throws PluginRuntimeException
   */
  public Plugin getPluginInstance(PluginDescriptor pDescriptor)
      throws PluginRuntimeException {
    if (fActivatedPlugins.containsKey(pDescriptor.getPluginId()))
      return fActivatedPlugins.get(pDescriptor.getPluginId());
    try {
      // Must synchronize here to make sure creation and initialization
      // of a plugin instance are done by one and only one thread.
      // The same is in Extension.getExtensionInstance().
      // Suggested by Stefan Groschupf <sg@media-style.com>
      synchronized (pDescriptor) {
        PluginClassLoader loader = pDescriptor.getClassLoader();
        Class<?> pluginClass = loader.loadClass(pDescriptor.getPluginClass());
        Constructor<?> constructor = pluginClass.getConstructor(new Class<?>[] {
            PluginDescriptor.class, Configuration.class });
        Plugin plugin = (Plugin) constructor.newInstance(new Object[] {
            pDescriptor, this.conf });
        plugin.startUp();
        fActivatedPlugins.put(pDescriptor.getPluginId(), plugin);
        return plugin;
      }
    } catch (ClassNotFoundException e) {
      throw new PluginRuntimeException(e);
    } catch (InstantiationException e) {
      throw new PluginRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new PluginRuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new PluginRuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new PluginRuntimeException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#finalize()
   */
  public void finalize() throws Throwable {
    shotDownActivatedPlugins();
  }

  /**
   * Shuts down all plugins
   * 
   * @throws PluginRuntimeException
   */
  private void shotDownActivatedPlugins() throws PluginRuntimeException {
    for (Plugin plugin : fActivatedPlugins.values()) {
      plugin.shutDown();
    }
  }

  private void displayStatus() {
    LOG.info("Plugin Auto-activation mode: [" + this.auto + "]");
    LOG.info("Registered Plugins:");

    if ((fRegisteredPlugins == null) || (fRegisteredPlugins.size() == 0)) {
      LOG.info("\tNONE");
    } else {
      for (PluginDescriptor plugin : fRegisteredPlugins) {
        LOG.info("\t" + plugin.getName() + " (" + plugin.getPluginId() + ")");
      }
    }

    LOG.info("Registered Extension-Points:");
    if ((fExtensionPoints == null) || (fExtensionPoints.size() == 0)) {
      LOG.info("\tNONE");
    } else {
      for (ExtensionPoint ep : fExtensionPoints.values()) {
        LOG.info("\t" + ep.getName() + " (" + ep.getId() + ")");
      }
    }
  }

  /**
   * Filters a list of plugins. The list of plugins is filtered regarding the
   * configuration properties <code>plugin.excludes</code> and
   * <code>plugin.includes</code>.
   * 
   * @param excludes
   * @param includes
   * @param plugins
   *          Map of plugins
   * @return map of plugins matching the configuration
   */
  private Map<String, PluginDescriptor> filter(Pattern excludes,
      Pattern includes, Map<String, PluginDescriptor> plugins) {

    Map<String, PluginDescriptor> map = new HashMap<String, PluginDescriptor>();

    if (plugins == null) {
      return map;
    }

    for (PluginDescriptor plugin : plugins.values()) {

      if (plugin == null) {
        continue;
      }
      String id = plugin.getPluginId();
      if (id == null) {
        continue;
      }

      if (!includes.matcher(id).matches()) {
        LOG.debug("not including: " + id);
        continue;
      }
      if (excludes.matcher(id).matches()) {
        LOG.debug("excluding: " + id);
        continue;
      }
      map.put(plugin.getPluginId(), plugin);
    }
    return map;
  }

  /**
   * Loads all necessary dependencies for a selected plugin, and then runs one
   * of the classes' main() method.
   * 
   * @param args
   *          plugin ID (needs to be activated in the configuration), and the
   *          class name. The rest of arguments is passed to the main method of
   *          the selected class.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err
          .println("Usage: PluginRepository pluginId className [arg1 arg2 ...]");
      return;
    }
    Configuration conf = NutchConfiguration.create();
    PluginRepository repo = new PluginRepository(conf);
    // args[0] - plugin ID
    PluginDescriptor d = repo.getPluginDescriptor(args[0]);
    if (d == null) {
      System.err.println("Plugin '" + args[0] + "' not present or inactive.");
      return;
    }
    ClassLoader cl = d.getClassLoader();
    // args[1] - class name
    Class<?> clazz = null;
    try {
      clazz = Class.forName(args[1], true, cl);
    } catch (Exception e) {
      System.err.println("Could not load the class '" + args[1] + ": "
          + e.getMessage());
      return;
    }
    Method m = null;
    try {
      m = clazz.getMethod("main", new Class<?>[] { args.getClass() });
    } catch (Exception e) {
      System.err.println("Could not find the 'main(String[])' method in class "
          + args[1] + ": " + e.getMessage());
      return;
    }
    String[] subargs = new String[args.length - 2];
    System.arraycopy(args, 2, subargs, 0, subargs.length);
    m.invoke(null, new Object[] { subargs });
  }
}

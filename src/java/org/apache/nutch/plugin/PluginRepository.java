/*
 /**
 * Copyright 2005 The Apache Software Foundation
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
package org.apache.nutch.plugin;

// JDK imports
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.WeakHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Pattern;

// Nutch imports
import org.apache.hadoop.util.LogFormatter;
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
 * 
 * @author joa23
 */
public class PluginRepository {
    private static final WeakHashMap CACHE = new WeakHashMap();

    private boolean auto;
    
    private List fRegisteredPlugins;

    private HashMap fExtensionPoints;

    private HashMap fActivatedPlugins;

    private Configuration conf;

    
    public static final Logger LOG = LogFormatter
            .getLogger("org.apache.nutch.plugin.PluginRepository");

    /**
     * @throws PluginRuntimeException
     * @see java.lang.Object#Object()
     */
    public PluginRepository(Configuration conf) throws RuntimeException {
      fActivatedPlugins = new HashMap();
      fExtensionPoints = new HashMap();
      this.conf = conf;
      this.auto = conf.getBoolean("plugin.auto-activation", true);
      String[] pluginFolders = conf.getStrings("plugin.folders");
      PluginManifestParser manifestParser =  new PluginManifestParser(conf, this);
      Map allPlugins =manifestParser.parsePluginFolder(pluginFolders);
      Pattern excludes = Pattern.compile(conf.get(
              "plugin.excludes", ""));
      Pattern includes = Pattern.compile(conf.get(
              "plugin.includes", ""));
      Map filteredPlugins = filter(excludes, includes, allPlugins);
      fRegisteredPlugins = getDependencyCheckedPlugins(
                              filteredPlugins,
                              this.auto ? allPlugins : filteredPlugins);
      installExtensionPoints(fRegisteredPlugins);
      try {
        installExtensions(fRegisteredPlugins);
      } catch (PluginRuntimeException e) {
         LOG.severe(e.toString());
         throw new RuntimeException(e.getMessage());
      }
      displayStatus();
    }

    /**
     * @return a cached instance of the plugin repository
     */
    public static synchronized PluginRepository get(Configuration conf) {
      PluginRepository result = (PluginRepository)CACHE.get(conf);
      if (result == null) {
        result = new PluginRepository(conf);
        CACHE.put(conf, result);
      }
      return result;
    }


    private void installExtensionPoints(List plugins) {
      if (plugins == null) { return; }
      
      for (int i=0; i<plugins.size(); i++) {
        PluginDescriptor plugin = (PluginDescriptor) plugins.get(i);
        ExtensionPoint[] points = plugin.getExtenstionPoints();
        for (int j=0; j<points.length; j++) {
          ExtensionPoint point = points[j];
          String xpId = point.getId();
          LOG.fine("Adding extension point " + xpId);
          fExtensionPoints.put(xpId, point);
        }
      }
    }
    
    /**
     * @param pRegisteredPlugins
     */
    private void installExtensions(List pRegisteredPlugins)
      throws PluginRuntimeException {
        
      for (int i = 0; i < pRegisteredPlugins.size(); i++) {
        PluginDescriptor descriptor = (PluginDescriptor) pRegisteredPlugins.get(i);
        Extension[] extensions = descriptor.getExtensions();
        for (int j = 0; j < extensions.length; j++) {
          Extension extension = extensions[j];
          String xpId = extension.getTargetPoint();
          ExtensionPoint point = getExtensionPoint(xpId);
          if (point == null) {
            throw new PluginRuntimeException(
                    "Plugin (" + descriptor.getPluginId() + "), " +
                    "extension point: " + xpId + " does not exist.");
          }
          point.addExtension(extension);
        }
      }
    }

    private void getPluginCheckedDependencies(PluginDescriptor plugin,
                                             Map plugins,
                                             Map dependencies,
                                             Map branch)
      throws MissingDependencyException,
             CircularDependencyException {
      
      if (dependencies == null) { dependencies = new HashMap(); }
      if (branch == null) { branch = new HashMap(); }
      branch.put(plugin.getPluginId(), plugin);
      
      // Get the plugin dependencies
      String[] ids = plugin.getDependencies();

      // Otherwise, checks each dependency
      for (int i=0; i<ids.length; i++) {
        String id = ids[i];
        PluginDescriptor dependency = (PluginDescriptor) plugins.get(id);
        if (dependency == null) {
          throw new MissingDependencyException(
                  "Missing dependency " + id +
                  " for plugin " + plugin.getPluginId());
        }
        if (branch.containsKey(id)) {
          throw new CircularDependencyException(
                  "Circular dependency detected " + id +
                  " for plugin " + plugin.getPluginId());
        }
        dependencies.put(id, dependency);
        getPluginCheckedDependencies((PluginDescriptor) plugins.get(id),
                                     plugins, dependencies, branch);
      }
      
      branch.remove(plugin.getPluginId());
    }

    private Map getPluginCheckedDependencies(PluginDescriptor plugin,
                                             Map plugins)
      throws MissingDependencyException,
             CircularDependencyException {
      Map dependencies = new HashMap();
      Map branch = new HashMap();
      getPluginCheckedDependencies(plugin, plugins, dependencies, branch);
      return dependencies;
    }
    
    /**
     * @param filtered is the list of plugin filtred
     * @param all is the list of all plugins found.
     * @return List
     */
    private List getDependencyCheckedPlugins(Map filtered, Map all) {
      if (filtered == null) { return null; }
      Map checked = new HashMap();
      Iterator iter = filtered.values().iterator();
      while (iter.hasNext()) {
        PluginDescriptor plugin = (PluginDescriptor) iter.next();
        try {
          checked.putAll(getPluginCheckedDependencies(plugin, all));
          checked.put(plugin.getPluginId(), plugin);
        } catch (MissingDependencyException mde) {
          // Simply ignore this plugin
          LOG.warning(mde.getMessage());
        } catch (CircularDependencyException cde) {
          // Simply ignore this plugin
          LOG.warning(cde.getMessage());
        }
      }
      return new ArrayList(checked.values());
    }

    /**
     * @param id
     * @param pLoadedPlugins
     * @return boolean
     */
    private boolean dependencyIsAvailable(String id, List pLoadedPlugins) {
        if (pLoadedPlugins != null && id != null) {
            for (int i = 0; i < pLoadedPlugins.size(); i++) {
                PluginDescriptor descriptor = (PluginDescriptor) pLoadedPlugins
                        .get(i);
                if (descriptor.getPluginId().equals(id)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns all registed plugin descriptors.
     * 
     * @return PluginDescriptor[]
     */
    public PluginDescriptor[] getPluginDescriptors() {
        return (PluginDescriptor[]) fRegisteredPlugins
                .toArray(new PluginDescriptor[fRegisteredPlugins.size()]);
    }

    /**
     * Returns the descriptor of one plugin identified by a plugin id.
     * 
     * @param pPluginId
     * @return PluginDescriptor
     */
    public PluginDescriptor getPluginDescriptor(String pPluginId) {
        for (int i = 0; i < fRegisteredPlugins.size(); i++) {
            PluginDescriptor descriptor = (PluginDescriptor) fRegisteredPlugins
                    .get(i);
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
        return (ExtensionPoint) this.fExtensionPoints.get(pXpId);
    }

    /**
     * Returns a instance of a plugin. Plugin instances are cached. So a plugin
     * exist only as one instance. This allow a central management of plugin own
     * resources.
     * 
     * After creating the plugin instance the startUp() method is invoked. The
     * plugin use a own classloader that is used as well by all instance of
     * extensions of the same plugin. This class loader use all exported
     * libraries from the dependend plugins and all plugin libraries.
     * 
     * @param pDescriptor
     * @return Plugin
     * @throws PluginRuntimeException
     */
    public Plugin getPluginInstance(PluginDescriptor pDescriptor)
            throws PluginRuntimeException {
        if (fActivatedPlugins.containsKey(pDescriptor.getPluginId()))
            return (Plugin) fActivatedPlugins.get(pDescriptor.getPluginId());
        try {
            // Must synchronize here to make sure creation and initialization
            // of a plugin instance are done by one and only one thread.
            // The same is in Extension.getExtensionInstance().
            // Suggested by Stefan Groschupf <sg@media-style.com>
            synchronized (pDescriptor) {
                PluginClassLoader loader = pDescriptor.getClassLoader();
                Class pluginClass = loader.loadClass(pDescriptor
                        .getPluginClass());
                Constructor constructor = pluginClass
                        .getConstructor(new Class[] { PluginDescriptor.class, Configuration.class });
                Plugin plugin = (Plugin) constructor
                        .newInstance(new Object[] { pDescriptor, this.conf });
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
     * @throws PluginRuntimeException
     */
    private void shotDownActivatedPlugins() throws PluginRuntimeException {
        Iterator iterator = fActivatedPlugins.keySet().iterator();
        while (iterator.hasNext()) {
            String pluginId = (String) iterator.next();
            Plugin object = (Plugin) fActivatedPlugins.get(pluginId);
            object.shutDown();
        }
    }
    
    private void displayStatus() {

      LOG.info("Plugin Auto-activation mode: [" + this.auto + "]");

      LOG.info("Registered Plugins:");
      if ((fRegisteredPlugins == null) || (fRegisteredPlugins.size() == 0)) {
        LOG.info("\tNONE");
      } else {
        for (int i=0; i<fRegisteredPlugins.size(); i++) {
          PluginDescriptor plugin = (PluginDescriptor) fRegisteredPlugins.get(i);
          LOG.info("\t" + plugin.getName() + " (" + plugin.getPluginId() + ")");
        }
      }

      LOG.info("Registered Extension-Points:");
      if ((fExtensionPoints == null) || (fExtensionPoints.size() == 0)) {
        LOG.info("\tNONE");
      } else {
        Iterator iter = fExtensionPoints.values().iterator();
        while (iter.hasNext()) {
          ExtensionPoint ep = (ExtensionPoint) iter.next();
          LOG.info("\t" + ep.getName() + " (" + ep.getId() + ")");
        }
      }
    }
    /**
     * Filters a list of plugins.
     * The list of plugins is filtered regarding the configuration
     * properties <code>plugin.excludes</code> and <code>plugin.includes</code>.
     */
    private Map filter(Pattern excludes, Pattern includes, Map plugins) {
      Map map = new HashMap();
      if (plugins == null) { return map; }

      Iterator iter = plugins.values().iterator();
      while (iter.hasNext()) {
        PluginDescriptor plugin = (PluginDescriptor) iter.next();
        if (plugin == null) { continue; }
        String id = plugin.getPluginId();
        if (id == null) { continue; }
        
        if (!includes.matcher(id).matches()) {
          LOG.fine("not including: " + id);
          continue;
        }
        if (excludes.matcher(id).matches()) {
          LOG.fine("excluding: " + id);
          continue;
        }
        map.put(plugin.getPluginId(), plugin);
      }
      return map;
    }

    /**
     * Loads all necessary dependencies for a selected plugin, and then
     * runs one of the classes' main() method.
     * @param args plugin ID (needs to be activated in the configuration), and
     * the class name. The rest of arguments is passed to the main method of the
     * selected class.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
      if (args.length < 2) {
        System.err.println("Usage: PluginRepository pluginId className [arg1 arg2 ...]");
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
      Class clazz = null;
      try {
        clazz = Class.forName(args[1], true, cl);
      } catch (Exception e) {
        System.err.println("Could not load the class '" + args[1] + ": " + e.getMessage());
        return;
      }
      Method m = null;
      try {
        m = clazz.getMethod("main", new Class[]{args.getClass()});
      } catch (Exception e) {
        System.err.println("Could not find the 'main(String[])' method in class " + args[1] + ": " + e.getMessage());
        return;
      }
      String[] subargs = new String[args.length - 2];
      System.arraycopy(args, 2, subargs, 0, subargs.length);
      m.invoke(null, new Object[]{subargs});
    }
}

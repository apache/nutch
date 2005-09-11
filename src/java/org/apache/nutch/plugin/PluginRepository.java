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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

// Nutch imports
import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.util.NutchConf;


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
  
    private final static boolean AUTO =
            NutchConf.get().getBoolean("plugin.auto-activation", true);
    
    private static PluginRepository fInstance;

    private List fRegisteredPlugins;

    private HashMap fExtensionPoints;

    private HashMap fActivatedPlugins;

    
    public static final Logger LOG = LogFormatter
            .getLogger("org.apache.nutch.plugin.PluginRepository");

    /**
     * @throws PluginRuntimeException
     * @see java.lang.Object#Object()
     */
    private PluginRepository() throws PluginRuntimeException{
      fActivatedPlugins = new HashMap();
      fExtensionPoints = new HashMap();
      Map allPlugins = PluginManifestParser.parsePluginFolder();
      Map filteredPlugins = PluginManifestParser.filter(allPlugins);
      fRegisteredPlugins = getDependencyCheckedPlugins(
                              filteredPlugins,
                              AUTO ? allPlugins : filteredPlugins);
      installExtensionPoints(fRegisteredPlugins);
      installExtensions(fRegisteredPlugins);
      displayStatus();
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
                                             Map plugins, Map dependencies)
      throws MissingDependencyException,
             CircularDependencyException {
      
      if (dependencies == null) { dependencies = new HashMap(); }
      
      String[] ids = plugin.getDependencies();
      for (int i=0; i<ids.length; i++) {
        String id = ids[i];
        PluginDescriptor dependency = (PluginDescriptor) plugins.get(id);
        if (dependency == null) {
          throw new MissingDependencyException(
                  "Missing dependency " + id +
                  " for plugin " + plugin.getPluginId());
        }
        if (dependencies.containsKey(id)) {
          throw new CircularDependencyException(
                  "Circular dependency detected " + id +
                  " for plugin " + plugin.getPluginId());
        }
        dependencies.put(id, dependency);
        getPluginCheckedDependencies((PluginDescriptor) plugins.get(id),
                                     plugins, dependencies);
      }
    }

    private Map getPluginCheckedDependencies(PluginDescriptor plugin,
                                             Map plugins)
      throws MissingDependencyException,
             CircularDependencyException {
      Map dependencies = new HashMap();
      getPluginCheckedDependencies(plugin, plugins, dependencies);
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
     * Returns the singelton instance of the <code>PluginRepository</code>
     */
    public static synchronized PluginRepository getInstance() {
        if (fInstance != null)
            return fInstance;
        try {
            fInstance = new PluginRepository();
        } catch (Exception e) {
            LOG.severe(e.toString());
            throw new RuntimeException(e);
        }
        return fInstance;
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
     */
    public ExtensionPoint getExtensionPoint(String pXpId) {
        return (ExtensionPoint) fExtensionPoints.get(pXpId);
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
                        .getConstructor(new Class[] { PluginDescriptor.class });
                Plugin plugin = (Plugin) constructor
                        .newInstance(new Object[] { pDescriptor });
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

      LOG.info("Plugin Auto-activation mode: [" + AUTO + "]");

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
}

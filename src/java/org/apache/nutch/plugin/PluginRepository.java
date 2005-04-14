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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.nutch.util.LogFormatter;

/**
 * The plugin repositority is a registry of all plugins.
 * 
 * At system boot up a repositority is builded by parsing the mainifest files of
 * all plugins. Plugins that require not existing other plugins are not
 * registed. For each plugin a plugin descriptor instance will be created. The
 * descriptor represent all meta information about a plugin. So a plugin
 * instance will be created later when it is required, this allow lazy plugin
 * loading.
 * 
 * @author joa23
 */
public class PluginRepository {
    private static PluginRepository fInstance;

    private ArrayList fRegisteredPlugins;

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
        fRegisteredPlugins = getDependencyCheckedPlugins(PluginManifestParser
                .parsePluginFolder());
        installExtensions(fRegisteredPlugins);
    }

    /**
     * @param fRegisteredPlugins
     */
    private void installExtensions(ArrayList pRegisteredPlugins)
            throws PluginRuntimeException {
        for (int i = 0; i < pRegisteredPlugins.size(); i++) {
            PluginDescriptor descriptor = (PluginDescriptor) pRegisteredPlugins
                    .get(i);
            Extension[] extensions = descriptor.getExtensions();
            for (int j = 0; j < extensions.length; j++) {
                Extension extension = extensions[j];
                String xpId = extension.getTargetPoint();
                ExtensionPoint point = getExtensionPoint(xpId);
                if (point == null)
                    throw new PluginRuntimeException("extension point: " + xpId
                            + " does not exist.");
                point.addExtension(extension);
            }
        }
    }

    /**
     * @param pLoadedPlugins
     * @return ArrayList
     */
    private ArrayList getDependencyCheckedPlugins(ArrayList pLoadedPlugins) {
        ArrayList availablePlugins = new ArrayList();
        for (int i = 0; i < pLoadedPlugins.size(); i++) {
            PluginDescriptor descriptor = (PluginDescriptor) pLoadedPlugins
                    .get(i);
            String[] dependencyIDs = descriptor.getDependencies();
            boolean available = true;
            for (int j = 0; j < dependencyIDs.length; j++) {
                String id = dependencyIDs[j];
                if (!dependencyIsAvailabel(id, pLoadedPlugins)) {
                    available = false;
                    //LOG.fine("Skipping " + descriptor.getName());
                    break;
                }
            }
            if (available) {
                //LOG.fine("Adding " + descriptor.getName());
                availablePlugins.add(descriptor);
                ExtensionPoint[] points = descriptor.getExtenstionPoints();
                for (int j = 0; j < points.length; j++) {
                    ExtensionPoint point = points[j];
                    String xpId = point.getId();
                    //LOG.fine("Adding extension point " + xpId);
                    fExtensionPoints.put(xpId, point);
                }
            }
        }
        return availablePlugins;
    }

    /**
     * @param id
     * @param pLoadedPlugins
     * @return boolean
     */
    private boolean dependencyIsAvailabel(String id, ArrayList pLoadedPlugins) {
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
}
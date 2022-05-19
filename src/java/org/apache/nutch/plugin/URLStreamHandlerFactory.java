/**
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

import java.lang.ref.WeakReference;
import java.net.URL;
import java.net.URLStreamHandler;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This URLStreamHandlerFactory knows about all the plugins
 * in use and thus can create the correct URLStreamHandler
 * even if it comes from a plugin classpath.
 * As the JVM allows only one instance of URLStreamHandlerFactory
 * to be registered, this class implements a singleton pattern.
 * @author Hiran Chaudhuri
 *
 */
public class URLStreamHandlerFactory
    implements java.net.URLStreamHandlerFactory {

  protected static final Logger LOG = LoggerFactory
      .getLogger(URLStreamHandlerFactory.class);

  /** The singleton instance. */
  private static URLStreamHandlerFactory instance;

  /** Here we register all PluginRepositories.
   * In this class we do not know why several instances of PluginRepository
   * are kept, nor do we know how long they will be used. To prevent
   * a memory leak, this class must not keep references to PluginRepository
   * but use WeakReference which allows PluginRepository to still be
   * garbage collected. The prize is we need to clean the list for
   * outdated references which is done in the {@link #removeInvalidRefs()} method.
   */
  private ArrayList<WeakReference<PluginRepository>> prs;

  static {
    instance = new URLStreamHandlerFactory();
    URL.setURLStreamHandlerFactory(instance);
    LOG.debug("Registered URLStreamHandlerFactory with the JVM.");
  }

  private URLStreamHandlerFactory() {
    this.prs = new ArrayList<>();
  }

  /** 
   * Get the singleton instance of this class.
   * @return a {@link org.apache.nutch.plugin.URLStreamHandlerFactory} instance
   */
  public static URLStreamHandlerFactory getInstance() {
    return instance;
  }

  /** Use this method once a new PluginRepository was created to register it.
   * 
   * @param pr The PluginRepository to be registered.
   */
  public void registerPluginRepository(PluginRepository pr) {
    this.prs.add(new WeakReference<PluginRepository>(pr));

    removeInvalidRefs();
  }

  @Override
  public URLStreamHandler createURLStreamHandler(String protocol) {
    LOG.debug("Creating URLStreamHandler for protocol: {}", protocol);

    removeInvalidRefs();

    // find the 'correct' PluginRepository. For now we simply take the first.
    // then ask it to return the URLStreamHandler
    for (WeakReference<PluginRepository> ref : this.prs) {
      PluginRepository pr = ref.get();
      if (pr != null) {
        // found PluginRepository. Let's get the URLStreamHandler...
        URLStreamHandler handler = pr.createURLStreamHandler(protocol);
        return handler;
      }
    }

    return null;
  }

  /**
   * Maintains the list of PluginRepositories by removing the references whose
   * referents have been garbage collected meanwhile.
   */
  private void removeInvalidRefs() {
    ArrayList<WeakReference<PluginRepository>> copy = new ArrayList<>(this.prs);
    for (WeakReference<PluginRepository> ref : copy) {
      if (ref.get() == null) {
        this.prs.remove(ref);
      }
    }
    LOG.debug("Removed '{}' invalid references. '{}' remaining.",
        copy.size() - this.prs.size(), this.prs.size());
  }
}

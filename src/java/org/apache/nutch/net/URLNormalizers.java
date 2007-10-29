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

package org.apache.nutch.net;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.util.ObjectCache;

/**
 * This class uses a "chained filter" pattern to run defined normalizers.
 * Different lists of normalizers may be defined for different "scopes", or
 * contexts where they are used (note however that they need to be activated
 * first through <tt>plugin.include</tt> property).
 * 
 * <p>There is one global scope defined by default, which consists of all
 * active normalizers. The order in which these normalizers
 * are executed may be defined in "urlnormalizer.order" property, which lists
 * space-separated implementation classes (if this property is missing normalizers
 * will be run in random order). If there are more
 * normalizers activated than explicitly named on this list, the remaining ones
 * will be run in random order after the ones specified on the list are executed.</p>
 * <p>You can define a set of contexts (or scopes) in which normalizers may be
 * called. Each scope can have its own list of normalizers (defined in
 * "urlnormalizer.scope.<scope_name>" property) and its own order (defined in
 * "urlnormalizer.order.<scope_name>" property). If any of these properties are
 * missing, default settings are used for the global scope.</p>
 * <p>In case no normalizers are required for any given scope, a
 * <code>org.apache.nutch.net.urlnormalizer.pass.PassURLNormalizer</code> should be used.</p>
 * <p>Each normalizer may further select among many configurations, depending on
 * the scope in which it is called, because the scope name is passed as a parameter
 * to each normalizer. You can also use the same normalizer for many scopes.</p>
 * <p>Several scopes have been defined, and various Nutch tools will attempt using
 * scope-specific normalizers first (and fall back to default config if scope-specific
 * configuration is missing).</p>
 * <p>Normalizers may be run several times, to ensure that modifications introduced
 * by normalizers at the end of the list can be further reduced by normalizers
 * executed at the beginning. By default this loop is executed just once - if you want
 * to ensure that all possible combinations have been applied you may want to run
 * this loop up to the number of activated normalizers. This loop count can be configured
 * through <tt>urlnormalizer.loop.count</tt> property. As soon as the url is
 * unchanged the loop will stop and return the result.</p>
 * 
 * @author Andrzej Bialecki
 */
public final class URLNormalizers {
  
  /** Default scope. If no scope properties are defined then the configuration for
   * this scope will be used.
   */
  public static final String SCOPE_DEFAULT = "default";
  /** Scope used by {@link org.apache.nutch.crawl.PartitionUrlByHost}. */
  public static final String SCOPE_PARTITION = "partition";
  /** Scope used by {@link org.apache.nutch.crawl.Generator}. */
  public static final String SCOPE_GENERATE_HOST_COUNT = "generate_host_count";
  /** Scope used by {@link org.apache.nutch.fetcher.Fetcher} when processing
   * redirect URLs.
   */
  public static final String SCOPE_FETCHER = "fetcher";
  /** Scope used when updating the CrawlDb with new URLs. */
  public static final String SCOPE_CRAWLDB = "crawldb";
  /** Scope used when updating the LinkDb with new URLs. */
  public static final String SCOPE_LINKDB = "linkdb";
  /** Scope used by {@link org.apache.nutch.crawl.Injector}. */
  public static final String SCOPE_INJECT = "inject";
  /** Scope used when constructing new {@link org.apache.nutch.parse.Outlink} instances. */
  public static final String SCOPE_OUTLINK = "outlink";
  

  public static final Log LOG = LogFactory.getLog(URLNormalizers.class);

  /* Empty extension list for caching purposes. */
  private final List<Extension> EMPTY_EXTENSION_LIST = Collections.EMPTY_LIST;
  
  private final URLNormalizer[] EMPTY_NORMALIZERS = new URLNormalizer[0];

  private Configuration conf;

  private ExtensionPoint extensionPoint;

  private URLNormalizer[] normalizers;
  
  private int loopCount;

  public URLNormalizers(Configuration conf, String scope) {
    this.conf = conf;
    this.extensionPoint = PluginRepository.get(conf).getExtensionPoint(
            URLNormalizer.X_POINT_ID);
    ObjectCache objectCache = ObjectCache.get(conf);
    
    if (this.extensionPoint == null) {
      throw new RuntimeException("x point " + URLNormalizer.X_POINT_ID
              + " not found.");
    }

    normalizers = (URLNormalizer[])objectCache.getObject(URLNormalizer.X_POINT_ID + "_" + scope);
    if (normalizers == null) {
      normalizers = getURLNormalizers(scope);
    }
    if (normalizers == EMPTY_NORMALIZERS) {
      normalizers = (URLNormalizer[])objectCache.getObject(URLNormalizer.X_POINT_ID + "_" + SCOPE_DEFAULT);
      if (normalizers == null) {
        normalizers = getURLNormalizers(SCOPE_DEFAULT);
      }
    }
    
    loopCount = conf.getInt("urlnormalizer.loop.count", 1);
  }

  /**
   * Function returns an array of {@link URLNormalizer}s for a given scope,
   * with a specified order.
   * 
   * @param scope
   *          The scope to return the <code>Array</code> of
   *          {@link URLNormalizer}s for.
   * @return An <code>Array</code> of {@link URLNormalizer}s for the given
   *         scope.
   * @throws PluginRuntimeException
   */
  URLNormalizer[] getURLNormalizers(String scope) {
    List<Extension> extensions = getExtensions(scope);
    ObjectCache objectCache = ObjectCache.get(conf);
    
    if (extensions == EMPTY_EXTENSION_LIST) {
      return EMPTY_NORMALIZERS;
    }
    
    List<URLNormalizer> normalizers = new Vector<URLNormalizer>(extensions.size());

    Iterator<Extension> it = extensions.iterator();
    while (it.hasNext()) {
      Extension ext = it.next();
      URLNormalizer normalizer = null;
      try {
        // check to see if we've cached this URLNormalizer instance yet
        normalizer = (URLNormalizer) objectCache.getObject(ext.getId());
        if (normalizer == null) {
          // go ahead and instantiate it and then cache it
          normalizer = (URLNormalizer) ext.getExtensionInstance();
          objectCache.setObject(ext.getId(), normalizer);
        }
        normalizers.add(normalizer);
      } catch (PluginRuntimeException e) {
        e.printStackTrace();
        LOG.warn("URLNormalizers:PluginRuntimeException when "
                + "initializing url normalizer plugin "
                + ext.getDescriptor().getPluginId()
                + " instance in getURLNormalizers "
                + "function: attempting to continue instantiating plugins");
      }
    }
    return normalizers.toArray(new URLNormalizer[normalizers
            .size()]);
  }

  /**
   * Finds the best-suited normalizer plugin for a given scope.
   * 
   * @param scope
   *          Scope for which we seek a normalizer plugin.
   * @return a list of extensions to be used for this scope. If none, returns
   *         empty list.
   * @throws PluginRuntimeException
   */
  private List<Extension> getExtensions(String scope) {
    ObjectCache objectCache = ObjectCache.get(conf);
    List<Extension> extensions = 
      (List<Extension>) objectCache.getObject(URLNormalizer.X_POINT_ID + "_x_"
                                                + scope);

    // Just compare the reference:
    // if this is the empty list, we know we will find no extension.
    if (extensions == EMPTY_EXTENSION_LIST) {
      return EMPTY_EXTENSION_LIST;
    }

    if (extensions == null) {
      extensions = findExtensions(scope);
      if (extensions != null) {
        objectCache.setObject(URLNormalizer.X_POINT_ID + "_x_" + scope, extensions);
      } else {
        // Put the empty extension list into cache
        // to remember we don't know any related extension.
        objectCache.setObject(URLNormalizer.X_POINT_ID + "_x_" + scope, EMPTY_EXTENSION_LIST);
        extensions = EMPTY_EXTENSION_LIST;
      }
    }
    return extensions;
  }

  /**
   * searches a list of suitable url normalizer plugins for the given scope.
   * 
   * @param scope
   *          Scope for which we seek a url normalizer plugin.
   * @return List - List of extensions to be used for this scope. If none,
   *         returns null.
   * @throws PluginRuntimeException
   */
  private List<Extension> findExtensions(String scope) {

    String[] orders = null;
    String orderlist = conf.get("urlnormalizer.order." + scope);
    if (orderlist == null) orderlist = conf.get("urlnormalizer.order");
    if (orderlist != null && !orderlist.trim().equals("")) {
      orders = orderlist.split("\\s+");
    }
    String scopelist = conf.get("urlnormalizer.scope." + scope);
    Set<String> impls = null;
    if (scopelist != null && !scopelist.trim().equals("")) {
      String[] names = scopelist.split("\\s+");
      impls = new HashSet<String>(Arrays.asList(names));
    }
    Extension[] extensions = this.extensionPoint.getExtensions();
    HashMap<String, Extension> normalizerExtensions = new HashMap<String, Extension>();
    for (int i = 0; i < extensions.length; i++) {
      Extension extension = extensions[i];
      if (impls != null && !impls.contains(extension.getClazz()))
        continue;
      normalizerExtensions.put(extension.getClazz(), extension);
    }
    List<Extension> res = new ArrayList<Extension>();
    if (orders == null) {
      res.addAll(normalizerExtensions.values());
    } else {
      // first add those explicitly named in correct order
      for (int i = 0; i < orders.length; i++) {
        Extension e = normalizerExtensions.get(orders[i]);
        if (e != null) {
          res.add(e);
          normalizerExtensions.remove(orders[i]);
        }
      }
      // then add all others in random order
      res.addAll(normalizerExtensions.values());
    }
    return res;
  }

  /**
   * Normalize
   * @param urlString The URL string to normalize.
   * @param scope The given scope.
   * @return A normalized String, using the given <code>scope</code>
   * @throws MalformedURLException If the given URL string is malformed.
   */
  public String normalize(String urlString, String scope)
          throws MalformedURLException {
    // optionally loop several times, and break if no further changes
    String initialString = urlString;
    for (int k = 0; k < loopCount; k++) {
      for (int i = 0; i < this.normalizers.length; i++) {
        if (urlString == null)
          return null;
        urlString = this.normalizers[i].normalize(urlString, scope);
      }
      if (initialString.equals(urlString)) break;
      initialString = urlString;
    }
    return urlString;
  }
}

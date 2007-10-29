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
package org.apache.nutch.analysis;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Nutch imports
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.util.ObjectCache;
import org.apache.hadoop.conf.Configuration;


/**
 * Creates and caches {@link NutchAnalyzer} plugins.
 *
 * @author J&eacute;r&ocirc;me Charron
 */
public class AnalyzerFactory {

  private final static String KEY = AnalyzerFactory.class.getName();
  
  public final static Log LOG = LogFactory.getLog(KEY);

  
  private NutchAnalyzer DEFAULT_ANALYZER;
  
  private ExtensionPoint extensionPoint;
  private Configuration conf;

  public AnalyzerFactory (Configuration conf) {
      DEFAULT_ANALYZER = new NutchDocumentAnalyzer(conf);
      this.conf = conf;
      this.extensionPoint = PluginRepository.get(conf).getExtensionPoint(NutchAnalyzer.X_POINT_ID);
      if(this.extensionPoint == null) {
          throw new RuntimeException("x point " + NutchAnalyzer.X_POINT_ID +
          " not found.");
      }
  }

  public static AnalyzerFactory get(Configuration conf) {
    ObjectCache objectCache = ObjectCache.get(conf);
    AnalyzerFactory factory = (AnalyzerFactory) objectCache.getObject(KEY);
    if (factory == null) {
      factory = new AnalyzerFactory(conf);
      objectCache.setObject(KEY, factory);
    }
    return factory;
  }
  
  /**
   * Returns the appropriate {@link NutchAnalyzer analyzer} implementation
   * given a language code.
   *
   * <p>NutchAnalyzer extensions should define the attribute "lang". The first
   * plugin found whose "lang" attribute equals the specified lang parameter is
   * used. If none match, then the {@link NutchDocumentAnalyzer} is used.
   */
  public NutchAnalyzer get(String lang) {

    NutchAnalyzer analyzer = DEFAULT_ANALYZER;
    Extension extension = getExtension(lang);
    if (extension != null) {
        try {
            analyzer = (NutchAnalyzer) extension.getExtensionInstance();
        } catch (PluginRuntimeException pre) {
            analyzer = DEFAULT_ANALYZER;
        }
    }
    return analyzer;
  }

  private Extension getExtension(String lang) {
    ObjectCache objectCache = ObjectCache.get(conf);
    if (lang == null) { return null; }
    Extension extension = (Extension) objectCache.getObject(lang);
    if (extension == null) {
      extension = findExtension(lang);
      if (extension != null) {
        objectCache.setObject(lang, extension);
      }
    }
    return extension;
  }

  private Extension findExtension(String lang) {

    if (lang != null) {
      Extension[] extensions = this.extensionPoint.getExtensions();
      for (int i=0; i<extensions.length; i++) {
        if (lang.equals(extensions[i].getAttribute("lang"))) {
          return extensions[i];
        }
      }
    }
    return null;
  }

  /** 
   * Method used by unit test
   */
  protected NutchAnalyzer getDefault() {
    return DEFAULT_ANALYZER;
  }

}

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
package org.apache.nutch.analysis;

// JDK imports
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

// Nutch imports
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.util.LogFormatter;


/**
 * Creates and caches {@link NutchAnalyzer} plugins.
 *
 * @author J&eacute;r&ocirc;me Charron
 */
public class AnalyzerFactory {

  public final static Logger LOG =
          LogFormatter.getLogger(AnalyzerFactory.class.getName());

  private final static ExtensionPoint X_POINT = 
          PluginRepository.getInstance()
                          .getExtensionPoint(NutchAnalyzer.X_POINT_ID);

  private final static Map CACHE = new HashMap();

  private final static NutchAnalyzer DEFAULT_ANALYSER = 
                                            new NutchDocumentAnalyzer();
  
  
  static {
    if (X_POINT == null) {
      throw new RuntimeException("x point " + NutchAnalyzer.X_POINT_ID +
                                 " not found.");
    }
  }


  private AnalyzerFactory() {}

  
  /**
   * Returns the appropriate {@link Analyser} implementation given a language
   * code.
   *
   * <p>NutchAnalyser extensions should define the attribute "lang". The first
   * plugin found whose "lang" attribute equals the specified lang parameter is
   * used. If none match, then the {@link NutchDocumentAnalyzer} is used.
   */
  public static NutchAnalyzer get(String lang) {

    NutchAnalyzer analyzer = DEFAULT_ANALYSER;
    Extension extension = getExtension(lang);
    if (extension != null) {
        try {
            analyzer = (NutchAnalyzer) extension.getExtensionInstance();
        } catch (PluginRuntimeException pre) {
            analyzer = DEFAULT_ANALYSER;
        }
    }
    return analyzer;
  }

  private static Extension getExtension(String lang) {

    Extension extension = (Extension) CACHE.get(lang);
    if (extension == null) {
      extension = findExtension(lang);
      CACHE.put(lang, extension);
    }
    return extension;
  }

  private static Extension findExtension(String lang) {

    if (lang != null) {
      Extension[] extensions = X_POINT.getExtentens();
      for (int i=0; i<extensions.length; i++) {
        if (lang.equals(extensions[i].getAttribute("lang"))) {
          return extensions[i];
        }
      }
    }
    return null;
  }

}

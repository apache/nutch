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
package org.apache.nutch.parse;

// JDK imports
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

// Nutch imports
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.util.LogFormatter;


/** Creates and caches {@link Parser} plugins.*/
public final class ParserFactory {
  
  public static final Logger LOG =
          LogFormatter.getLogger(ParserFactory.class.getName());
  
  public static final String DEFAULT_PLUGIN = "*";

  private static final ExtensionPoint X_POINT =
          PluginRepository.getInstance().getExtensionPoint(Parser.X_POINT_ID);
  
  private static final ParsePluginList PARSE_PLUGIN_LIST =
          new ParsePluginsReader().parse();
  
  
  static {
    if (X_POINT == null) {
      throw new RuntimeException("x point "+Parser.X_POINT_ID+" not found.");
    }
    if (PARSE_PLUGIN_LIST == null) {
      throw new RuntimeException("Parse Plugins preferences could not be loaded.");
    }
  }
  
  private static final Hashtable CACHE = new Hashtable();
  
  
  private ParserFactory() {}                      // no public ctor
  
  /**
   * Returns the appropriate {@link Parser} implementation given a content
   * type and url.
   *
   * <p>Parser extensions should define the attributes"contentType" and/or
   * "pathSuffix".  Content type has priority: the first plugin found whose
   * "contentType" attribute matches the beginning of the content's type is
   * used.  If none match, then the first whose "pathSuffix" attribute matches
   * the end of the url's path is used.  If neither of these match, then the
   * first plugin whose "pathSuffix" is the empty string is used.
   */
  public static Parser getParser(String contentType, String url)
  throws ParserNotFound {
    
    try {
      Extension extension = getExtension(contentType);
      if (extension != null) {
        return (Parser) extension.getExtensionInstance();
      }
      // TODO once the MimeTypes is available
      // extension = getExtension(MimeUtils.map(contentType));
      // if (extension != null) {
      //   return (Parser) extension.getExtensionInstance();
      // }
      // Last Chance: Guess content-type from file url...
      // extension = getExtension(MimeUtils.getMimeType(url));
        throw new ParserNotFound(url, contentType);
    } catch (PluginRuntimeException e) {
      throw new ParserNotFound(url, contentType, e.toString());
    }
  }
    
  protected static Extension getExtension(String contentType)
  throws PluginRuntimeException {
    
    Extension extension = (Extension) CACHE.get(contentType);
    if (extension == null) {
      extension = findExtension(contentType);
      // TODO: For null extension, add a fake extension in the CACHE
      //       in order to avoid trying to find each time
      //       an unavailable extension
      if (extension != null) {
        CACHE.put(contentType, extension);
      }
    }
    return extension;
  }
  
  private static Extension findExtension(String contentType)
  throws PluginRuntimeException{
    
    Extension[] extensions = X_POINT.getExtensions();
    
    // Look for a preferred plugin.
    List parsePluginList = PARSE_PLUGIN_LIST.getPluginList(contentType);
    Extension extension = matchExtension(parsePluginList, extensions, contentType);
    if (extension != null) {
      return extension;
    }
    
    // If none found, look for a default plugin.
    parsePluginList = PARSE_PLUGIN_LIST.getPluginList(DEFAULT_PLUGIN);
    return matchExtension(parsePluginList, extensions, DEFAULT_PLUGIN);
  }
  
  private static Extension matchExtension(List plugins,
                                          Extension[] extensions,
                                          String contentType) {
    
    // Preliminary check
    if (plugins == null) { return null; }
    
    Iterator iter = plugins.iterator();
    while (iter.hasNext()) {
      String pluginId = (String) iter.next();
      if (pluginId != null) {
        for (int i=0; i<extensions.length; i++) {
          if (match(extensions[i], pluginId, contentType)) {
            return extensions[i];
          }
        }
      }
    }
    return null;
  }

  private static boolean match(Extension extension, String id, String type) {
    return (id.equals(extension.getDescriptor().getPluginId())) &&
              (type.equals(extension.getAttribute("contentType")) ||
              (type.equals(DEFAULT_PLUGIN))); 
  }
}

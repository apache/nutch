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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;

// Nutch imports
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.mime.MimeType;
import org.apache.nutch.util.mime.MimeTypeException;

/** Creates and caches {@link Parser} plugins.*/
public final class ParserFactory {
  
  public static final Logger LOG =
          LogFormatter.getLogger(ParserFactory.class.getName());
  
  /** Wildcard for default plugins. */
  public static final String DEFAULT_PLUGIN = "*";
  
  /** Empty extension list for caching purposes. */
  private final List EMPTY_EXTENSION_LIST = Collections.EMPTY_LIST;
  
  private Configuration conf;
  private ExtensionPoint extensionPoint;
  private ParsePluginList parsePluginList;

  public ParserFactory(Configuration conf) {
    this.conf = conf;
    this.extensionPoint = PluginRepository.get(conf).getExtensionPoint(
        Parser.X_POINT_ID);
    this.parsePluginList = new ParsePluginsReader().parse(conf);

    if (this.extensionPoint == null) {
      throw new RuntimeException("x point " + Parser.X_POINT_ID + " not found.");
    }
    if (this.parsePluginList == null) {
      throw new RuntimeException(
          "Parse Plugins preferences could not be loaded.");
    }
  }                      
  

  /**
   * Returns the appropriate {@link Parser} implementation given a content type
   * and url.
   * 
   * @deprecated Since the addition of NUTCH-88, this method is replaced by
   *             taking the highest priority {@link Parser} returned from
   *             {@link #getParsers(String, String)}.
   * 
   * Parser extensions should define the attributes "contentType" and/or
   * "pathSuffix". Content type has priority: the first plugin found whose
   * "contentType" attribute matches the beginning of the content's type is
   * used. If none match, then the first whose "pathSuffix" attribute matches
   * the end of the url's path is used. If neither of these match, then the
   * first plugin whose "pathSuffix" is the empty string is used.
   */
  public Parser getParser(String contentType, String url)
  throws ParserNotFound {
    
    Parser[] parsers = getParsers(contentType, url);
    
    if(parsers != null){
      //give the user the highest priority parser available
      for(int i = 0;  i < parsers.length; i++ ){
        Parser p = parsers[i];
        if(p != null){
          return p;
        }
      }
      
      throw new ParserNotFound(url, contentType);
      
    } 
    else{
      throw new ParserNotFound(url, contentType);
    }
  }
   
  /**
   * Function returns an array of {@link Parser}s for a given content type.
   *
   * The function consults the internal {@link ParsePluginList} for the
   * ParserFactory to determine the list of pluginIds, then gets the
   * appropriate extension points to instantiate as {Parser}s.
   *
   * @param contentType The contentType to return the <code>Array</code>
   *                    of {Parser}s for.
   * @param url The url for the content that may allow us to get the type from
   *            the file suffix.
   * @return An <code>Array</code> of {@Parser}s for the given contentType.
   *         If there were plugins mapped to a contentType via the
   *         <code>parse-plugins.xml</code> file, but never enabled via
   *         the <code>plugin.includes</code> Nutch conf, then those plugins
   *         won't be part of this array, i.e., they will be skipped.
   *         So, if the ordered list of parsing plugins for
   *         <code>text/plain</code> was <code>[parse-text,parse-html,
   *         parse-rtf]</code>, and only <code>parse-html</code> and
   *         <code>parse-rtf</code> were enabled via
   *         <code>plugin.includes</code>, then this ordered Array would
   *         consist of two {@link Parser} interfaces,
   *         <code>[parse-html, parse-rtf]</code>.
   */
  public Parser[] getParsers(String contentType, String url)
  throws ParserNotFound {
    
    List parsers = null;
    List parserExts = null;
    
    // TODO once the MimeTypes is available
    // parsers = getExtensions(MimeUtils.map(contentType));
    // if (parsers != null) {
    //   return parsers;
    // }
    // Last Chance: Guess content-type from file url...
    // parsers = getExtensions(MimeUtils.getMimeType(url));

    parserExts = getExtensions(contentType);
    if (parserExts == null) {
      throw new ParserNotFound(url, contentType);
    }

    parsers = new Vector(parserExts.size());
    for (Iterator i=parserExts.iterator(); i.hasNext(); ){
      Extension ext = (Extension) i.next();
      Parser p = null;
      try {
        //check to see if we've cached this parser instance yet
        p = (Parser) this.conf.getObject(ext.getDescriptor().getPluginId());
        if (p == null) {
          // go ahead and instantiate it and then cache it
          p = (Parser) ext.getExtensionInstance();
          this.conf.setObject(ext.getDescriptor().getPluginId(),p);
        }
        parsers.add(p);
      } catch (PluginRuntimeException e) {
          e.printStackTrace();
        LOG.warning("ParserFactory:PluginRuntimeException when "
                  + "initializing parser plugin "
                  + ext.getDescriptor().getPluginId()
                  + " instance in getParsers "
                  + "function: attempting to continue instantiating parsers");
      }
    }
    return (Parser[]) parsers.toArray(new Parser[]{});
  }
  
  /**
   * <p>
   * Function returns a {@link Parser} instance with the specified
   * <code>parserId</code>. If the Parser instance isn't found, then the
   * function throws a <code>ParserNotFound</code> exception. If the function
   * is able to find the {@link Parser} in the internal
   * <code>PARSER_CACHE</code> then it will return the already instantiated
   * Parser. Otherwise, if it has to instantiate the Parser itself , then this
   * function will cache that Parser in the internal <code>PARSER_CACHE</code>.
   * 
   * @param parserId
   *          The string ID (e.g., "parse-text", "parse-msword") of the
   *          {@link Parser} implementation to return.
   * @return A {@link Parser} implementation specified by the parameter
   *         <code>parserId</code>.
   * @throws ParserNotFound
   *           If the Parser is not found (i.e., registered with the extension
   *           point), or if the there a {@link PluginRuntimeException}
   *           instantiating the {@link Parser}.
   */
  public Parser getParserById(String parserId) throws ParserNotFound {
    // first check the cache

    if (this.conf.getObject(parserId) != null) {
      return (Parser) this.conf.getObject(parserId);
    } else {
      // get the list of registered parsing extensions
      // then find the right one by Id

      Extension[] extensions = this.extensionPoint.getExtensions();
      Extension parserExt = getExtensionById(extensions, parserId);

      if (parserExt == null) {
        throw new ParserNotFound("No Parser Found for parserId: " + parserId
            + "!");
      } else {
        // instantiate the Parser
        try {
          Parser p = null;
          p = (Parser) parserExt.getExtensionInstance();
          this.conf.setObject(parserId, p);
          return p;
        } catch (PluginRuntimeException e) {
          LOG.warning("ParserFactory:PluginRuntimeException when "
              + "initializing parser plugin "
              + parserExt.getDescriptor().getPluginId()
              + " instance in getParserById");
          throw new ParserNotFound("No Parser Found for parserId: " + parserId
              + "!");
        }
      }
    }
  }
  
  /**
   * finds the best-suited parse plugin for a given contentType.
   * 
   * @param contentType
   *          Content-Type for which we seek a parse plugin.
   * @return List - List of extensions to be used for this contentType. If none,
   *         returns null.
   */
  protected List getExtensions(String contentType) {
    
    // First of all, tries to clean the content-type
    String type = null;
    try {
        type = MimeType.clean(contentType);
    } catch (MimeTypeException mte) {
        LOG.info("Could not clean the content-type [" + contentType +
                 "], Reason is [" + mte + "]. Using its raw version...");
        type = contentType;
    }

    List extensions = (List) this.conf.getObject(type);

    // Just compare the reference:
    // if this is the empty list, we know we will find no extension.
    if (extensions == EMPTY_EXTENSION_LIST) {
      return null;
    }
    
    if (extensions == null) {
      extensions = findExtensions(type);
      if (extensions != null) {
        this.conf.setObject(type, extensions);
      } else {
      	// Put the empty extension list into cache
      	// to remember we don't know any related extension.
      	this.conf.setObject(type, EMPTY_EXTENSION_LIST);
      }
    }
    return extensions;
  }
  
  /**
   * searches a list of suitable parse plugins for the given contentType.
   * <p>It first looks for a preferred plugin defined in the parse-plugin
   * file.  If none is found, it returns a list of default plugins.
   * 
   * @param contentType Content-Type for which we seek a parse plugin.
   * @return List - List of extensions to be used for this contentType.
   *                If none, returns null.
   */
  private List findExtensions(String contentType) {
    
    Extension[] extensions = this.extensionPoint.getExtensions();
    
    // Look for a preferred plugin.
    List parsePluginList = this.parsePluginList.getPluginList(contentType);
    List extensionList = matchExtensions(parsePluginList, extensions, contentType);
    if (extensionList != null) {
      return extensionList;
    }
    
    // If none found, look for a default plugin.
    parsePluginList = this.parsePluginList.getPluginList(DEFAULT_PLUGIN);
    return matchExtensions(parsePluginList, extensions, DEFAULT_PLUGIN);
  }
  
  /**
   * Tries to find a suitable parser for the given contentType.
   * <ol>
   * <li>It checks if a parser which accepts the contentType
   * can be found in the <code>plugins</code> list;</li>
   * <li>If this list is empty, it tries to find amongst the loaded
   * extensions whether some of them might suit and warns the user.</li>
   * </ol>
   * @param plugins List of candidate plugins.
   * @param extensions Array of loaded extensions.
   * @param contentType Content-Type for which we seek a parse plugin.
   * @return List - List of extensions to be used for this contentType.
   *                If none, returns null.
   */
  private List matchExtensions(List plugins,
                                      Extension[] extensions,
                                      String contentType) {
    
    List extList = null;
    if (plugins != null) {
      extList = new Vector(plugins.size());
      
      for (Iterator i = plugins.iterator(); i.hasNext();) {
        String parsePluginId = (String) i.next();
        
        Extension ext = getExtensionByIdAndType(extensions,
                                                parsePluginId,
                                                contentType);
        // the extension returned may be null
        // that means that it was not enabled in the plugin.includes
        // nutch conf property, but it was mapped in the
        // parse-plugins.xml
        // file. 
        // OR it was enabled in plugin.includes, but the plugin's plugin.xml
        // file does not claim that the plugin supports the specified mimeType
        // in either case, LOG the appropriate error message to WARN level
        
        if (ext == null) {
           //try to get it just by its pluginId
            ext = getExtensionById(extensions, parsePluginId);
          if (ext != null) {
            // plugin was enabled via plugin.includes
            // its plugin.xml just doesn't claim to support that
            // particular mimeType
            LOG.warning("ParserFactory:Plugin: " + parsePluginId +
                        " mapped to contentType " + contentType +
                        " via parse-plugins.xml, but " + "its plugin.xml " +
                        "file does not claim to support contentType: " +
                        contentType);
            
            //go ahead and load the extension anyways, though
            extList.add(ext);
          
          } else{
            // plugin wasn't enabled via plugin.includes
            LOG.warning("ParserFactory: Plugin: " + parsePluginId + 
                        " mapped to contentType " + contentType +
                        " via parse-plugins.xml, but not enabled via " +
                        "plugin.includes in nutch-default.xml");                     
          }
          
        } else{
          // add it to the list
          extList.add(ext);
        }
      }
      
      return extList;
    } else {
      // okay, there were no list of plugins defined for
      // this mimeType, however, there may be plugins registered
      // via the plugin.includes nutch conf property that claim
      // via their plugin.xml file to support this contentType
      // so, iterate through the list of extensions and if you find
      // any extensions where this is the case, throw a
      // NotMappedParserException
      
      List unmappedPlugins = new Vector();
      
      for (int i = 0; i < extensions.length; i++) {
        if (extensions[i].getAttribute("contentType") != null
            && extensions[i].getAttribute("contentType").equals(
                contentType)) {
          unmappedPlugins.add(extensions[i].getDescriptor()
              .getPluginId());
        }
      }
      
      if (unmappedPlugins.size() > 0) {
        LOG.info("The parsing plugins: " + unmappedPlugins +
                 " are enabled via the plugin.includes system " +
                 "property, and all claim to support the content type " +
                 contentType + ", but they are not mapped to it  in the " +
                 "parse-plugins.xml file");
      } else {
        LOG.fine("ParserFactory:No parse plugins mapped or enabled for " +
                 "contentType " + contentType);
      }
      return null;
    }
  }

  private boolean match(Extension extension, String id, String type) {
    return (id.equals(extension.getDescriptor().getPluginId())) &&
    (type.equals(extension.getAttribute("contentType")) ||
        (type.equals(DEFAULT_PLUGIN))); 
  }
  
  private Extension getExtensionByIdAndType(Extension[] extList,
                                                   String plugId,
                                                   String contentType) {
    for (int i = 0; i < extList.length; i++) {
      if (match(extList[i], plugId, contentType)) {
        return extList[i];
      }
    }
    return null;
  }
  
  private Extension getExtensionById(Extension[] extList, String plugId) {
    for(int i = 0; i < extList.length; i++){
      if(plugId.equals(extList[i].getDescriptor().getPluginId())){
        return extList[i];
      }
    }
    return null;
  }
}

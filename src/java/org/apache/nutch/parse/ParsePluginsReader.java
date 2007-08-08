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
package org.apache.nutch.parse;

// JDK imports
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;

// Nutch imports
import org.apache.nutch.util.NutchConfiguration;


/**
 * A reader to load the information stored in the
 * <code>$NUTCH_HOME/conf/parse-plugins.xml</code> file.
 *
 * @author mattmann
 * @version 1.0
 */
class ParsePluginsReader {
  
  /* our log stream */
  public static final Log LOG = LogFactory.getLog(ParsePluginsReader.class);
  
  /** The property name of the parse-plugins location */
  private static final String PP_FILE_PROP = "parse.plugin.file";

  /** the parse-plugins file */
  private String fParsePluginsFile = null;

  
  /**
   * Constructs a new ParsePluginsReader
   */
  public ParsePluginsReader() { }
  
  /**
   * Reads the <code>parse-plugins.xml</code> file and returns the
   * {@link #ParsePluginList} defined by it.
   *
   * @return A {@link #ParsePluginList} specified by the
   *         <code>parse-plugins.xml</code> file.
   * @throws Exception
   *             If any parsing error occurs.
   */
  public ParsePluginList parse(Configuration conf) {
    
    ParsePluginList pList = new ParsePluginList();
    
    // open up the XML file
    DocumentBuilderFactory factory = null;
    DocumentBuilder parser = null;
    Document document = null;
    InputSource inputSource = null;
    
    InputStream ppInputStream = null;
    if (fParsePluginsFile != null) {
      URL parsePluginUrl = null;
      try {
        parsePluginUrl = new URL(fParsePluginsFile);
        ppInputStream = parsePluginUrl.openStream();
      } catch (Exception e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Unable to load parse plugins file from URL " +
                   "[" + fParsePluginsFile + "]. Reason is [" + e + "]");
        }
        return pList;
      }
    } else {
      ppInputStream = conf.getConfResourceAsInputStream(
                          conf.get(PP_FILE_PROP));
    }
    
    inputSource = new InputSource(ppInputStream);
    
    try {
      factory = DocumentBuilderFactory.newInstance();
      parser = factory.newDocumentBuilder();
      document = parser.parse(inputSource);
    } catch (Exception e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Unable to parse [" + fParsePluginsFile + "]." +
                 "Reason is [" + e + "]");
      }
      return null;
    }
    
    Element parsePlugins = document.getDocumentElement();
    
    // build up the alias hash map
    Map<String, String> aliases = getAliases(parsePlugins);
    // And store it on the parse plugin list
    pList.setAliases(aliases);
     
    // get all the mime type nodes
    NodeList mimeTypes = parsePlugins.getElementsByTagName("mimeType");
    
    // iterate through the mime types
    for (int i = 0; i < mimeTypes.getLength(); i++) {
      Element mimeType = (Element) mimeTypes.item(i);
      String mimeTypeStr = mimeType.getAttribute("name");
      
      // for each mimeType, get the plugin list
      NodeList pluginList = mimeType.getElementsByTagName("plugin");
      
      // iterate through the plugins, add them in order read
      // OR if they have a special order="" attribute, then hold those in
      // a separate list, and then insert them into the final list at the
      // order specified
      if (pluginList != null && pluginList.getLength() > 0) {
        List<String> plugList = new ArrayList<String>(pluginList.getLength());
        
        for (int j = 0; j<pluginList.getLength(); j++) {
          Element plugin = (Element) pluginList.item(j);
          String pluginId = plugin.getAttribute("id");
          String extId = aliases.get(pluginId);
          if (extId == null) {
            // Assume an extension id is directly specified
            extId = pluginId;
          }
          String orderStr = plugin.getAttribute("order");
          int order = -1;
          try {
            order = Integer.parseInt(orderStr);
          } catch (NumberFormatException ignore) {
          }
          if (order != -1) {
            plugList.add(order - 1, extId);
          } else {
            plugList.add(extId);
          }
        }
        
        // now add the plugin list and map it to this mimeType
        pList.setPluginList(mimeTypeStr, plugList);
        
      } else if (LOG.isWarnEnabled()) {
        LOG.warn("ParsePluginsReader:ERROR:no plugins defined for mime type: "
                 + mimeTypeStr + ", continuing parse");
      }
    }
    return pList;
  }
  
  /**
   * Tests parsing of the parse-plugins.xml file. An alternative name for the
   * file can be specified via the <code>--file</code> option, although the
   * file must be located in the <code>$NUTCH_HOME/conf</code> directory.
   *
   * @param args
   *            Currently only the --file argument to specify an alternative
   *            name for the parse-plugins.xml file is supported.
   */
  public static void main(String[] args) throws Exception {
    String parsePluginFile = null;
    String usage = "ParsePluginsReader [--file <parse plugin file location>]";
    
    if (( args.length != 0 && args.length != 2 )
        || (args.length == 2 && !"--file".equals(args[0]))) {
      System.err.println(usage);
      System.exit(1);
    }
    
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--file")) {
        parsePluginFile = args[++i];
      }
    }
    
    ParsePluginsReader reader = new ParsePluginsReader();
    
    if (parsePluginFile != null) {
      reader.setFParsePluginsFile(parsePluginFile);
    }
    
    ParsePluginList prefs = reader.parse(NutchConfiguration.create());
    
    for (String mimeType : prefs.getSupportedMimeTypes()) {
      
      System.out.println("MIMETYPE: " + mimeType);
      List<String> plugList = prefs.getPluginList(mimeType);
      
      System.out.println("EXTENSION IDs:");
      
      for (String j : plugList) {
        System.out.println(j);
      }
    }
    
  }
  
  /**
   * @return Returns the fParsePluginsFile.
   */
  public String getFParsePluginsFile() {
    return fParsePluginsFile;
  }
  
  /**
   * @param parsePluginsFile
   *            The fParsePluginsFile to set.
   */
  public void setFParsePluginsFile(String parsePluginsFile) {
    fParsePluginsFile = parsePluginsFile;
  }
  
  private Map<String, String> getAliases(Element parsePluginsRoot) {

    Map<String, String> aliases = new HashMap<String, String>();
    NodeList aliasRoot = parsePluginsRoot.getElementsByTagName("aliases");
	  
    if (aliasRoot == null || (aliasRoot != null && aliasRoot.getLength() == 0)) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("No aliases defined in parse-plugins.xml!");
      }
      return aliases;
    }
	  
    if (aliasRoot.getLength() > 1) {
      // log a warning, but try and continue processing
      if (LOG.isWarnEnabled()) {
        LOG.warn("There should only be one \"aliases\" tag in parse-plugins.xml");
      }
    }
	  
    Element aliasRootElem = (Element)aliasRoot.item(0);
    NodeList aliasElements = aliasRootElem.getElementsByTagName("alias");
	  
    if (aliasElements != null && aliasElements.getLength() > 0) {
      for (int i=0; i<aliasElements.getLength(); i++) {
        Element aliasElem = (Element)aliasElements.item(i);
	String parsePluginId = aliasElem.getAttribute("name");
	String extensionId = aliasElem.getAttribute("extension-id");
        if (LOG.isTraceEnabled()) {
          LOG.trace("Found alias: plugin-id: " + parsePluginId +
                    ", extension-id: " + extensionId);
        }
        if (parsePluginId != null && extensionId != null) {
          aliases.put(parsePluginId, extensionId);
        }
      }
    }
    return aliases;
  }
  
}

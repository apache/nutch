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
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;
import java.io.InputStream;
import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

// Nutch imports
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;


/**
 * A reader to load the information stored in the
 * <code>$NUTCH_HOME/conf/parse-plugins.xml</code> file.
 *
 * @author mattmann
 * @version 1.0
 */
public class ParsePluginsReader {
  
  /* our log stream */
  public static final Logger LOG = 
          LogFormatter.getLogger(ParsePluginsReader.class.getName());
  
  /** The property name of the parse-plugins location */
  private static final String PP_FILE_PROP = "parse.plugin.file";

  /* the parse-plugins file */
  private String fParsePluginsFile = null;

  
  /**
   * Constructs a new ParsePluginsReader
   */
  public ParsePluginsReader() { }
  
  /**
   * Reads the <code>parse-plugins.xml</code> file and returns the
   * {@link ParsePluginList} defined by it.
   *
   * @return A {@link ParsePluginList} specified by the
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
        LOG.warning("Unable to load parse plugins file from URL " +
                    "[" + fParsePluginsFile + "]. Reason is [" + e + "]");
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
      LOG.warning("Unable to parse [" + fParsePluginsFile + "]." +
                  "Reason is [" + e + "]");
      return null;
    }
    
    Element parsePlugins = document.getDocumentElement();
    
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
      // a
      // separate list, and then insert them into the final list at the
      // order
      // specified
      
      if (pluginList != null && pluginList.getLength() > 0) {
        List plugList = new Vector(pluginList.getLength());
        
        for (int j = 0; j < pluginList.getLength(); j++) {
          Element plugin = (Element) pluginList.item(j);
          String pluginId = plugin.getAttribute("id");
          
          String orderStr = plugin.getAttribute("order");
          int order = -1;
          
          try {
            order = Integer.parseInt(orderStr);
          } catch (NumberFormatException ignore) {
          }
          
          if (order != -1) {
            plugList.add(order - 1, pluginId);
          } else {
            plugList.add(pluginId);
          }
        }
        
        // now add the plugin list and map it to this mimeType
        pList.setPluginList(mimeTypeStr, plugList);
        
      } else {
        LOG.warning("ParsePluginsReader:ERROR:no plugins defined for mime type: "
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
    
    for (Iterator i = prefs.getSupportedMimeTypes().iterator(); i.hasNext();) {
      String mimeType = (String) i.next();
      
      System.out.println("MIMETYPE: " + mimeType);
      List plugList = prefs.getPluginList(mimeType);
      
      System.out.println("PLUGINS:");
      
      for (Iterator j = plugList.iterator(); j.hasNext();) {
        System.out.println((String) j.next());
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
  
}

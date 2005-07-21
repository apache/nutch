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

// $Id: PrefixURLFilter.java,v 1.2 2005/02/07 19:10:37 cutting Exp $

package org.apache.nutch.net;

import org.apache.nutch.util.NutchConf;
import org.apache.nutch.util.LogFormatter;

import org.apache.nutch.util.PrefixStringMatcher;
import org.apache.nutch.util.TrieStringMatcher;

import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;

import java.io.Reader;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * Filters URLs based on a file of URL prefixes. The file is named by
 * (1) property "urlfilter.prefix.file" in ./conf/nutch-default.xml, and
 * (2) attribute "file" in plugin.xml of this plugin
 * Attribute "file" has higher precedence if defined.
 *
 * <p>The format of this file is one URL prefix per line.</p>
 */
public class PrefixURLFilter implements URLFilter {

  private static final Logger LOG =
    LogFormatter.getLogger(PrefixURLFilter.class.getName());

  // read in attribute "file" of this plugin.
  private static String attributeFile = null;
  static {
    String pluginName = "urlfilter-prefix";
    Extension[] extensions = PluginRepository.getInstance()
      .getExtensionPoint(URLFilter.class.getName()).getExtensions();
    for (int i=0; i < extensions.length; i++) {
      Extension extension = extensions[i];
      if (extension.getDiscriptor().getPluginId().equals(pluginName)) {
        attributeFile = extension.getAttribute("file");
        break;
      }
    }
    if (attributeFile != null && attributeFile.trim().equals(""))
      attributeFile = null;
    if (attributeFile != null) {
      LOG.info("Attribute \"file\" is defined for plugin "+pluginName+" as "+attributeFile);
    } else {
      //LOG.warning("Attribute \"file\" is not defined in plugin.xml for plugin "+pluginName);
    }
  }

  private TrieStringMatcher trie;

  public PrefixURLFilter() throws IOException {
    String file = NutchConf.get().get("urlfilter.prefix.file");
    // attribute "file" takes precedence if defined
    if (attributeFile != null)
      file = attributeFile;
    Reader reader = NutchConf.get().getConfResourceAsReader(file);

    if (reader == null) {
      trie = new PrefixStringMatcher(new String[0]);
    } else {
      trie = readConfigurationFile(reader);
    }
  }

  public PrefixURLFilter(String filename) throws IOException {
    trie = readConfigurationFile(new FileReader(filename));
  }

  public String filter(String url) {
    if (trie.shortestMatch(url) == null)
      return null;
    else
      return url;
  }

  private static TrieStringMatcher readConfigurationFile(Reader reader)
    throws IOException {
    
    BufferedReader in=new BufferedReader(reader);
    List urlprefixes = new ArrayList();
    String line;

    while((line=in.readLine())!=null) {
      if (line.length() == 0)
        continue;

      char first=line.charAt(0);
      switch (first) {
      case ' ' : case '\n' : case '#' :           // skip blank & comment lines
        continue;
      default :
	urlprefixes.add(line);
      }
    }

    return new PrefixStringMatcher(urlprefixes);
  }

  public static void main(String args[])
    throws IOException {
    
    PrefixURLFilter filter;
    if (args.length >= 1)
      filter = new PrefixURLFilter(args[0]);
    else
      filter = new PrefixURLFilter();
    
    BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
    String line;
    while((line=in.readLine())!=null) {
      String out=filter.filter(line);
      if(out!=null) {
        System.out.println(out);
      }
    }
  }
  
}

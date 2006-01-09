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

package org.apache.nutch.net;

import org.apache.nutch.util.NutchConf;
import org.apache.nutch.util.LogFormatter;

import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;

import java.io.Reader;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.regex.*;

/**
 * Filters URLs based on a file of regular expressions. The file is named by
 * (1) property "urlfilter.regex.file" in ./conf/nutch-default.xml, and
 * (2) attribute "file" in plugin.xml of this plugin
 * Attribute "file" has higher precedence if defined.
 *
 * <p>The format of this file is:
 * <pre>
 * [+-]<regex>
 * </pre>
 * where plus means go ahead and index it and minus means no.
 */

public class RegexURLFilter implements URLFilter {

  private static final Logger LOG =
    LogFormatter.getLogger(RegexURLFilter.class.getName());

  // read in attribute "file" of this plugin.
  private static String attributeFile = null;
  static {
    String pluginName = "urlfilter-regex";
    Extension[] extensions = PluginRepository.getInstance()
      .getExtensionPoint(URLFilter.class.getName()).getExtensions();
    for (int i=0; i < extensions.length; i++) {
      Extension extension = extensions[i];
      if (extension.getDescriptor().getPluginId().equals(pluginName)) {
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

  private static class Rule {
    public Pattern pattern;
    public boolean sign;
    public String regex;
  }

  private List rules;

  public RegexURLFilter() throws IOException, PatternSyntaxException {
    String file = NutchConf.get().get("urlfilter.regex.file");
    // attribute "file" takes precedence if defined
    if (attributeFile != null)
      file = attributeFile;
    Reader reader = NutchConf.get().getConfResourceAsReader(file);

    if (reader == null) {
      LOG.severe("Can't find resource: " + file);
    } else {
      rules=readConfigurationFile(reader);
    }
  }

  public RegexURLFilter(String filename)
    throws IOException, PatternSyntaxException {
    rules = readConfigurationFile(new FileReader(filename));
  }

  public synchronized String filter(String url) {
    Iterator i=rules.iterator();
    while(i.hasNext()) {
      Rule r=(Rule) i.next();
      Matcher matcher = r.pattern.matcher(url);

      if (matcher.find()) {
        //System.out.println("Matched " + r.regex);
        return r.sign ? url : null;
      }
    };
        
    return null;   // assume no go
  }

  //
  // Format of configuration file is
  //    
  // [+-]<regex>
  //
  // where plus means go ahead and index it and minus means no.
  // 

  private static List readConfigurationFile(Reader reader)
    throws IOException, PatternSyntaxException {

    BufferedReader in=new BufferedReader(reader);
    List rules=new ArrayList();
    String line;
       
    while((line=in.readLine())!=null) {
      if (line.length() == 0)
        continue;
      char first=line.charAt(0);
      boolean sign=false;
      switch (first) {
      case '+' : 
        sign=true;
        break;
      case '-' :
        sign=false;
        break;
      case ' ' : case '\n' : case '#' :           // skip blank & comment lines
        continue;
      default :
        throw new IOException("Invalid first character: "+line);
      }

      String regex=line.substring(1);

      Rule rule=new Rule();
      rule.pattern=Pattern.compile(regex);
      rule.sign=sign;
      rule.regex=regex;
      rules.add(rule);
    }

    return rules;
  }

  public static void main(String args[])
    throws IOException, PatternSyntaxException {

    RegexURLFilter filter=new RegexURLFilter();
    BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
    String line;
    while((line=in.readLine())!=null) {
      String out=filter.filter(line);
      if(out!=null) {
        System.out.print("+");
        System.out.println(out);
      } else {
        System.out.print("-");
        System.out.println(line);
      }
    }
  }

}

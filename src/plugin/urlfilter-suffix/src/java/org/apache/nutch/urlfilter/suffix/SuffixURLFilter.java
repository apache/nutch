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

package org.apache.nutch.urlfilter.suffix;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.*;

import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.SuffixStringMatcher;

import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.StringReader;

import java.util.List;
import java.util.ArrayList;

import java.net.URL;
import java.net.MalformedURLException;

/**
 * Filters URLs based on a file of URL suffixes. The file is named by
 * <ol>
 * <li>property "urlfilter.suffix.file" in ./conf/nutch-default.xml, and</li>
 * <li>attribute "file" in plugin.xml of this plugin</li>
 * </ol>
 * Attribute "file" has higher precedence if defined. If the config file is
 * missing, all URLs will be rejected.
 * 
 * <p>This filter can be configured to work in one of two modes:
 * <ul>
 * <li><b>default to reject</b> ('-'): in this mode, only URLs that match suffixes
 * specified in the config file will be accepted, all other URLs will be
 * rejected.</li>
 * <li><b>default to accept</b> ('+'): in this mode, only URLs that match suffixes
 * specified in the config file will be rejected, all other URLs will be
 * accepted.</li>
 * </ul>
 * <p>
 * The format of this config file is one URL suffix per line, with no preceding
 * whitespace. Order, in which suffixes are specified, doesn't matter. Blank
 * lines and comments (#) are allowed.
 * </p>
 * <p>
 * A single '+' or '-' sign not followed by any suffix must be used once, to
 * signify the mode this plugin operates in. An optional single 'I' can be appended,
 * to signify that suffix matches should be case-insensitive. The default, if 
 * not specified, is to use case-sensitive matches, i.e. suffix '.JPG'
 * does not match '.jpg'.
 * </p>
 * <p>
 * NOTE: the format of this file is different from urlfilter-prefix, because
 * that plugin doesn't support allowed/prohibited prefixes (only supports
 * allowed prefixes). Please note that this plugin does not support regular
 * expressions, it only accepts literal suffixes. I.e. a suffix "+*.jpg" is most
 * probably wrong, you should use "+.jpg" instead.
 * </p>
 * <h4>Example 1</h4>
 * <p>
 * The configuration shown below will accept all URLs with '.html' or '.htm'
 * suffixes (case-sensitive - '.HTML' or '.HTM' will be rejected),
 * and prohibit all other suffixes.
 * <p>
 * 
 * <pre>
 *  # this is a comment
 *  
 *  # prohibit all unknown, case-sensitive matching
 *  -
 *
 *  # collect only HTML files.
 *  .html
 *  .htm
 * </pre>
 * 
 * </p>
 * <h4>Example 2</h4>
 * <p>
 * The configuration shown below will accept all URLs except common graphical
 * formats.
 * <p>
 * 
 * <pre>
 *  # this is a comment
 *  
 *  # allow all unknown, case-insensitive matching
 *  +I
 *  
 *  # prohibited suffixes
 *  .gif
 *  .png
 *  .jpg
 *  .jpeg
 *  .bmp
 * </pre>
 * 
 * </p>
 * @author Andrzej Bialecki
 */
public class SuffixURLFilter implements URLFilter {

  private static final Logger LOG = LoggerFactory.getLogger(SuffixURLFilter.class);

  // read in attribute "file" of this plugin.
  private String attributeFile = null;

  private SuffixStringMatcher suffixes;
  private boolean modeAccept = false;
  private boolean filterFromPath = false;
  private boolean ignoreCase = false;

  private Configuration conf;

  public SuffixURLFilter() throws IOException {

  }

  public SuffixURLFilter(Reader reader) throws IOException {
    readConfiguration(reader);
  }

  public String filter(String url) {
    if (url == null) return null;
    String _url;
    if (ignoreCase)
      _url = url.toLowerCase();
    else _url = url;
    if (filterFromPath) {
      try {
        URL pUrl = new URL(_url);
        _url = pUrl.getPath();
      } catch (MalformedURLException e) {
        // don't care
      }
    }

    String a = suffixes.shortestMatch(_url);
    if (a == null) {
      if (modeAccept) return url;
      else return null;
    } else {
      if (modeAccept) return null;
      else return url;
    }
  }

  public void readConfiguration(Reader reader) throws IOException {

    // handle missing config file
    if (reader == null) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Missing urlfilter.suffix.file, all URLs will be rejected!");
      }
      suffixes = new SuffixStringMatcher(new String[0]);
      modeAccept = false;
      ignoreCase = false;
      return;
    }
    BufferedReader in = new BufferedReader(reader);
    List<String> aSuffixes = new ArrayList<String>();
    boolean allow = false;
    boolean ignore = false;
    String line;

    while ((line = in.readLine()) != null) {
      if (line.length() == 0) continue;

      char first = line.charAt(0);
      switch (first) {
        case ' ':
        case '\n':
        case '#': // skip blank & comment lines
          break;
        case '-':
          allow = false;
          if(line.contains("P"))
            filterFromPath = true;
          if(line.contains("I"))
            ignore = true;
          break;
        case '+':
          allow = true;
          if(line.contains("P"))
            filterFromPath = true;
          if(line.contains("I"))
            ignore = true;
          break;
        default:
          aSuffixes.add(line);
      }
    }
    if (ignore) {
      for (int i = 0; i < aSuffixes.size(); i++) {
        aSuffixes.set(i, ((String) aSuffixes.get(i)).toLowerCase());
      }
    }
    suffixes = new SuffixStringMatcher(aSuffixes);
    modeAccept = allow;
    ignoreCase = ignore;
  }

  public static void main(String args[]) throws IOException {

    SuffixURLFilter filter;
    if (args.length >= 1)
      filter = new SuffixURLFilter(new FileReader(args[0]));
    else {
      filter = new SuffixURLFilter();
      filter.setConf(NutchConfiguration.create());
    }

    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;
    while ((line = in.readLine()) != null) {
      String out = filter.filter(line);
      if (out != null) {
        System.out.println("ACCEPTED " + out);
      } else {
        System.out.println("REJECTED " + out);
      }
    }
  }

  public void setConf(Configuration conf) {
    this.conf = conf;

    String pluginName = "urlfilter-suffix";
    Extension[] extensions = PluginRepository.get(conf).getExtensionPoint(URLFilter.class.getName()).getExtensions();
    for (int i = 0; i < extensions.length; i++) {
      Extension extension = extensions[i];
      if (extension.getDescriptor().getPluginId().equals(pluginName)) {
        attributeFile = extension.getAttribute("file");
        break;
      }
    }
    if (attributeFile != null && attributeFile.trim().equals("")) attributeFile = null;
    if (attributeFile != null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Attribute \"file\" is defined for plugin " + pluginName + " as " + attributeFile);
      }
    } else {
      // if (LOG.isWarnEnabled()) {
      //   LOG.warn("Attribute \"file\" is not defined in plugin.xml for
      //   plugin "+pluginName);
      // }
    }

    String file = conf.get("urlfilter.suffix.file");
    String stringRules = conf.get("urlfilter.suffix.rules");
    // attribute "file" takes precedence if defined
    if (attributeFile != null) file = attributeFile;
    Reader reader = null;
    if (stringRules != null) { // takes precedence over files
      reader = new StringReader(stringRules);
    } else {
      reader = conf.getConfResourceAsReader(file);
    }

    try {
      readConfiguration(reader);
    } catch (IOException e) {
      if (LOG.isErrorEnabled()) { LOG.error(e.getMessage()); }
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  public boolean isModeAccept() {
    return modeAccept;
  }

  public void setModeAccept(boolean modeAccept) {
    this.modeAccept = modeAccept;
  }

  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  public void setIgnoreCase(boolean ignoreCase) {
    this.ignoreCase = ignoreCase;
  }

  public void setFilterFromPath(boolean filterFromPath) {
    this.filterFromPath = filterFromPath;
  }
}

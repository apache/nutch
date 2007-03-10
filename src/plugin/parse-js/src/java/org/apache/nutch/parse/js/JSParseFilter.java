/*
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
package org.apache.nutch.parse.js;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * <p>
 * This class is a heuristic link extractor for JavaScript files and code
 * snippets. The general idea of a two-pass regex matching comes from Heritrix.
 * Parts of the code come from OutlinkExtractor.java by Stephan Strittmatter.
 * </p>
 * 
 * <p>
 * This Filter extracts javascript from following locations:
 * </p>
 * <li>from inside &lt;script> tags</li>
 * <li>from html 4.0 events like Window: onload,onunload, Form:
 * onchange,onsubmit,onreset,onselect,onblur,onfocus Keyboard:
 * onkeydown,onkeypress,onkeyup Mouse:
 * onclick,ondbclick,onmousedown,onmouseout,onmousover,onmouseup
 * </li>
 * <li>a href starting with literal "javascript"</li>
 * 
 * 
 * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
 */
public class JSParseFilter implements HtmlParseFilter, Parser {
  public static final Log LOG = LogFactory.getLog(JSParseFilter.class);

  private static final int MAX_TITLE_LEN = 80;

  private Configuration conf;
  
  public Parse filter(Content content, Parse parse, HTMLMetaTags metaTags, DocumentFragment doc) {
    String url = content.getBaseUrl();
    ArrayList outlinks = new ArrayList();
    walk(doc, parse, metaTags, url, outlinks);
    if (outlinks.size() > 0) {
      Outlink[] old = parse.getData().getOutlinks();
      String title = parse.getData().getTitle();
      List list = Arrays.asList(old);
      outlinks.addAll(list);
      ParseStatus status = parse.getData().getStatus();
      String text = parse.getText();
      Outlink[] newlinks = (Outlink[])outlinks.toArray(new Outlink[outlinks.size()]);
      ParseData parseData = new ParseData(status, title, newlinks,
                                          parse.getData().getContentMeta(),
                                          parse.getData().getParseMeta());
      parseData.setConf(this.conf);
      parse = new ParseImpl(text, parseData);
    }
    return parse;
  }
  
  private void walk(Node n, Parse parse, HTMLMetaTags metaTags, String base, List outlinks) {
    if (n instanceof Element) {
      String name = n.getNodeName();
      if (name.equalsIgnoreCase("script")) {
        String lang = null;
        Node lNode = n.getAttributes().getNamedItem("language");
        if (lNode == null) lang = "javascript";
        else lang = lNode.getNodeValue();
        //XXX lang is not checked??
        StringBuffer script = new StringBuffer();
        NodeList nn = n.getChildNodes();
        if (nn.getLength() > 0) {
          for (int i = 0; i < nn.getLength(); i++) {
            if (i > 0) script.append('\n');
            script.append(nn.item(i).getNodeValue());
          }
          if (LOG.isDebugEnabled()) {
            LOG.info("script: language=" + lang + ", text: " + script.toString());
          }
          Outlink[] links = getJSLinks(script.toString(), "", base);
          if (links != null && links.length > 0) outlinks.addAll(Arrays.asList(links));
          // no other children of interest here, go one level up.
          return;
        }
      } else {
        // process all HTML 4.0 events, if present...
        NamedNodeMap attrs = n.getAttributes();
        int len = attrs.getLength();
        for (int i = 0; i < len; i++) {
          // Window: onload,onunload
          // Form: onchange,onsubmit,onreset,onselect,onblur,onfocus
          // Keyboard: onkeydown,onkeypress,onkeyup
          // Mouse: onclick,ondbclick,onmousedown,onmouseout,onmousover,onmouseup
          Node anode = attrs.item(i);
          Outlink[] links = null;
          if (anode.getNodeName().startsWith("on")) {
            links = getJSLinks(anode.getNodeValue(), "", base);
          } else if (anode.getNodeName().equalsIgnoreCase("href")) {
            String val = anode.getNodeValue();
            if (val != null && val.toLowerCase().indexOf("javascript:") != -1) {
              links = getJSLinks(val, "", base);
            }
          }
          if (links != null && links.length > 0) outlinks.addAll(Arrays.asList(links));
        }
      }
    }
    NodeList nl = n.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++) {
      walk(nl.item(i), parse, metaTags, base, outlinks);
    }
  }
  
  public Parse getParse(Content c) {
    String type = c.getContentType();
    if (type != null && !type.trim().equals("") && !type.toLowerCase().startsWith("application/x-javascript"))
      return new ParseStatus(ParseStatus.FAILED_INVALID_FORMAT,
              "Content not JavaScript: '" + type + "'").getEmptyParse(getConf());
    String script = new String(c.getContent());
    Outlink[] outlinks = getJSLinks(script, "", c.getUrl());
    if (outlinks == null) outlinks = new Outlink[0];
    // Title? use the first line of the script...
    String title;
    int idx = script.indexOf('\n');
    if (idx != -1) {
      if (idx > MAX_TITLE_LEN) idx = MAX_TITLE_LEN;
      title = script.substring(0, idx);
    } else {
      idx = Math.min(MAX_TITLE_LEN, script.length());
      title = script.substring(0, idx);
    }
    ParseData pd = new ParseData(ParseStatus.STATUS_SUCCESS, title, outlinks,
                                 c.getMetadata());
    pd.setConf(this.conf);
    Parse parse = new ParseImpl(script, pd);
    return parse;
  }
  
  private static final String STRING_PATTERN = "(\\\\*(?:\"|\'))([^\\s\"\']+?)(?:\\1)";
  // A simple pattern. This allows also invalid URL characters.
  private static final String URI_PATTERN = "(^|\\s*?)/?\\S+?[/\\.]\\S+($|\\s*)";
  // Alternative pattern, which limits valid url characters.
  //private static final String URI_PATTERN = "(^|\\s*?)[A-Za-z0-9/](([A-Za-z0-9$_.+!*,;/?:@&~=-])|%[A-Fa-f0-9]{2})+[/.](([A-Za-z0-9$_.+!*,;/?:@&~=-])|%[A-Fa-f0-9]{2})+(#([a-zA-Z0-9][a-zA-Z0-9$_.+!*,;/?:@&~=%-]*))?($|\\s*)";
  
  /**
   *  This method extracts URLs from literals embedded in JavaScript.
   */
  Outlink[] getJSLinks(String plainText, String anchor, String base) {

    final List outlinks = new ArrayList();
    URL baseURL = null;
    
    try {
      baseURL = new URL(base);
    } catch (Exception e) {
      if (LOG.isErrorEnabled()) { LOG.error("getJSLinks", e); }
    }

    try {
      final Pattern stringPattern = Pattern.compile(STRING_PATTERN,
          Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
      final Pattern urlPattern = Pattern.compile(URI_PATTERN,
              Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
      
      final Matcher quoted = stringPattern.matcher(plainText);

      String url;

      //loop the matches
      while (quoted.find()) {
        String quotedString = quoted.group(2);
        Matcher urls = urlPattern.matcher(quotedString);
        
        if (!urls.find()) {
          //if (LOG.isTraceEnabled()) { LOG.trace(" - invalid '" + url + "'"); }
          continue;
        }

        url = urls.group();
        
        if (url.startsWith("www.")) {
            url = "http://" + url;
        } else {
          // See if candidate URL is parseable.  If not, pass and move on to
          // the next match.
          try {
            url = new URL(baseURL, url).toString();
          } catch (MalformedURLException ex) {
            if (LOG.isTraceEnabled()) {
              LOG.trace(" - failed URL parse '" + url + "' and baseURL '" +
                  baseURL + "'", ex);
            }
            continue;
          }
        }
        url = url.replaceAll("&amp;", "&");
        if (LOG.isTraceEnabled()) {
          LOG.trace(" - outlink from JS: '" + url + "'");
        }
        outlinks.add(new Outlink(url, anchor, getConf()));
      }
    } catch (Exception ex) {
      // if it is a malformed URL we just throw it away and continue with
      // extraction.
      if (LOG.isErrorEnabled()) { LOG.error("getJSLinks", ex); }
    }

    final Outlink[] retval;

    //create array of the Outlinks
    if (outlinks != null && outlinks.size() > 0) {
      retval = (Outlink[]) outlinks.toArray(new Outlink[0]);
    } else {
      retval = new Outlink[0];
    }

    return retval;
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println(JSParseFilter.class.getName() + " file.js baseURL");
      return;
    }
    InputStream in = new FileInputStream(args[0]);
    BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
    StringBuffer sb = new StringBuffer();
    String line = null;
    while ((line = br.readLine()) != null) sb.append(line + "\n");
    JSParseFilter parseFilter = new JSParseFilter();
    parseFilter.setConf(NutchConfiguration.create());
    Outlink[] links = parseFilter.getJSLinks(sb.toString(), "", args[1]);
    System.out.println("Outlinks extracted: " + links.length);
    for (int i = 0; i < links.length; i++)
      System.out.println(" - " + links[i]);
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }
}

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
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.ParseFilter;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.PatternMatcherInput;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * This class is a heuristic link extractor for JavaScript files and
 * code snippets. The general idea of a two-pass regex matching comes from
 * Heritrix. Parts of the code come from OutlinkExtractor.java
 * by Stephan Strittmatter.
 *
 * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
 */
public class JSParseFilter implements ParseFilter, Parser {
  public static final Logger LOG = LoggerFactory.getLogger(JSParseFilter.class);

  private static final int MAX_TITLE_LEN = 80;

  private Configuration conf;

  /**
   * Scan the JavaScript looking for possible {@link Outlink}'s
   * @param url URL of the {@link WebPage} to be parsed 
   * @param page {@link WebPage} object relative to the URL
   * @param parse {@link Parse} object holding parse status
   * @param metatags within the {@link NutchDocument}
   * @param doc The {@link NutchDocument} object
   * @return parse the actual {@link Parse} object
   */
  @Override
  public Parse filter(String url, WebPage page, Parse parse,
      HTMLMetaTags metaTags, DocumentFragment doc) {

    ArrayList<Outlink> outlinks = new ArrayList<Outlink>();
    walk(doc, parse, metaTags, url, outlinks);
    if (outlinks.size() > 0) {
      Outlink[] old = parse.getOutlinks();
      String title = parse.getTitle();
      List<Outlink> list = Arrays.asList(old);
      outlinks.addAll(list);
      ParseStatus status = parse.getParseStatus();
      String text = parse.getText();
      Outlink[] newlinks = outlinks.toArray(new Outlink[outlinks.size()]);
      return new Parse(text, title, newlinks, status);
    }
    return parse;
  }

  private void walk(Node n, Parse parse, HTMLMetaTags metaTags, String base, List<Outlink> outlinks) {
    if (n instanceof Element) {
      String name = n.getNodeName();
      if (name.equalsIgnoreCase("script")) {
        @SuppressWarnings("unused")
        String lang = null;
        Node lNode = n.getAttributes().getNamedItem("language");
        if (lNode == null) lang = "javascript";
        else lang = lNode.getNodeValue();
        StringBuffer script = new StringBuffer();
        NodeList nn = n.getChildNodes();
        if (nn.getLength() > 0) {
          for (int i = 0; i < nn.getLength(); i++) {
            if (i > 0) script.append('\n');
            script.append(nn.item(i).getNodeValue());
          }
          // This logging makes the output very messy.
          //if (LOG.isInfoEnabled()) {
          //  LOG.info("script: language=" + lang + ", text: " + script.toString());
          //}
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

  /**
   * Set the {@link Configuration} object
   * @param url URL of the {@link WebPage} which is parsed
   * @param page {@link WebPage} object relative to the URL
   * @return parse the actual {@link Parse} object
   */
  @Override
  public Parse getParse(String url, WebPage page) {
    String type = TableUtil.toString(page.getContentType());
    if (type != null && !type.trim().equals("") && !type.toLowerCase().startsWith("application/x-javascript"))
      return ParseStatusUtils.getEmptyParse(ParseStatusCodes.FAILED_INVALID_FORMAT,
          "Content not JavaScript: '" + type + "'", getConf());
    String script = Bytes.toString(page.getContent());
    Outlink[] outlinks = getJSLinks(script, "", url);
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
    Parse parse =
      new Parse(script, title, outlinks, ParseStatusUtils.STATUS_SUCCESS);
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
  private Outlink[] getJSLinks(String plainText, String anchor, String base) {

    final List<Outlink> outlinks = new ArrayList<Outlink>();
    URL baseURL = null;

    try {
      baseURL = new URL(base);
    } catch (Exception e) {
      if (LOG.isErrorEnabled()) { 
        LOG.error("error assigning base URL", e); 
      }
    }

    try {
      final PatternCompiler cp = new Perl5Compiler();
      final Pattern pattern = cp.compile(STRING_PATTERN,
          Perl5Compiler.CASE_INSENSITIVE_MASK | Perl5Compiler.READ_ONLY_MASK
          | Perl5Compiler.MULTILINE_MASK);
      final Pattern pattern1 = cp.compile(URI_PATTERN,
          Perl5Compiler.CASE_INSENSITIVE_MASK | Perl5Compiler.READ_ONLY_MASK
          | Perl5Compiler.MULTILINE_MASK);
      final PatternMatcher matcher = new Perl5Matcher();

      final PatternMatcher matcher1 = new Perl5Matcher();
      final PatternMatcherInput input = new PatternMatcherInput(plainText);

      MatchResult result;
      String url;

      //loop the matches
      while (matcher.contains(input, pattern)) {
        result = matcher.getMatch();
        url = result.group(2);
        PatternMatcherInput input1 = new PatternMatcherInput(url);
        if (!matcher1.matches(input1, pattern1)) {
          if (LOG.isTraceEnabled()) { 
        	LOG.trace(" - invalid '" + url + "'"); 
          }
          continue;
        }
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
        outlinks.add(new Outlink(url, anchor));
      }
    } catch (Exception ex) {
      // if it is a malformed URL we just throw it away and continue with
      // extraction.
      if (LOG.isErrorEnabled()) { 
        LOG.error(" - invalid or malformed URL", ex); 
      }
    }

    final Outlink[] retval;

    //create array of the Outlinks
    if (outlinks != null && outlinks.size() > 0) {
      retval = outlinks.toArray(new Outlink[0]);
    } else {
      retval = new Outlink[0];
    }

    return retval;
  }

  /**
   * Main method which can be run from command line with the plugin option.
   * The method takes two arguments e.g. o.a.n.parse.js.JSParseFilter file.js baseURL  
   * @param args
   * @throws Exception
   */
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

  /**
   * Set the {@link Configuration} object
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Gets all the fields for a given {@link WebPage}
   * Many datastores need to setup the mapreduce job by specifying the fields
   * needed. All extensions that work on WebPage are able to specify what fields
   * they need.
   */
  @Override
  public Collection<WebPage.Field> getFields() {
    return null;
  }

}

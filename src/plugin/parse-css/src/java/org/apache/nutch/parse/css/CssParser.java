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

package org.apache.nutch.parse.css;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.steadystate.css.parser.CSSOMParser;
import com.steadystate.css.parser.SACParserCSS3;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.parse.*;
import org.apache.nutch.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.css.sac.CSSParseException;
import org.w3c.css.sac.InputSource;
import org.w3c.dom.css.*;


public class CssParser implements org.apache.nutch.parse.Parser {
  public static final Logger LOG = LoggerFactory.getLogger(CssParser.class);

  /**
   * Suppresses warnings, logs all errors, and throws fatal errors
   */
  private class ErrorHandler implements org.w3c.css.sac.ErrorHandler {
    @Override
    public void warning(CSSParseException exception) { }

    @Override
    public void error(CSSParseException exception) {
      LOG.debug("CSS parser error: {}", exception.getMessage());
    }

    @Override
    public void fatalError(CSSParseException exception) {
      error(exception);
      throw exception;
    }
  }

  private Configuration config;

  /** Parses CSS stylesheets for outlinks.
   *
   * Extracts :
   *
   *   - \\@import url(...) ...;
   *   - \\@import '...';
   *   - \\@font-face src: <uri>
   *   - and all non-custom style property values matching url(...)
   *
   * Ignores:
   *
   *   - \\@namespace <uri>
   *   - \\@document <url>
   *
   * @param content CSS resource content to be parsed
   * @return result of parse
   */
  @Override
  public ParseResult getParse(Content content) {
    CSSOMParser parser;
    parser = new CSSOMParser(new SACParserCSS3());
    parser.setErrorHandler(new ErrorHandler());

    URL base;
    try {
      base = new URL(content.getBaseUrl());
    } catch (MalformedURLException e) {
      return new ParseStatus(e)
        .getEmptyParseResult(content.getUrl(), getConf());
    }

    ByteArrayInputStream bis = new ByteArrayInputStream(content.getContent());
    Reader reader = new InputStreamReader(bis, StandardCharsets.UTF_8);
    InputSource source = new InputSource(reader);
    CSSStyleSheet sheet;
    try {
      sheet = parser.parseStyleSheet(source, null, content.getBaseUrl());
    } catch (IOException e) {
      return new ParseStatus(e)
        .getEmptyParseResult(content.getUrl(), getConf());
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        LOG.warn("failed to close reader");
      }
    }

    CSSRuleList rules = sheet.getCssRules();
    List<String> urls = new ArrayList<String>();
    for (int i = 0; i < rules.getLength(); i++) {
      CSSRule rule = rules.item(i);
      switch (rule.getType()) {
        // @import
        case CSSRule.IMPORT_RULE:
          urls.add(((CSSImportRule) rule).getHref());
          break;

        // @font-face
        case CSSRule.FONT_FACE_RULE:
          collectStyleDeclarationOutlinks(((CSSFontFaceRule)rule).getStyle(), urls);
          break;

        // normal CSS style rule
        case CSSRule.STYLE_RULE:
          collectStyleDeclarationOutlinks(((CSSStyleRule)rule).getStyle(), urls);
          break;

        // ignore @charset, @media, @page and unknown at-rules
        default:
          break;
      }
    }

    // resolve each relative URL to create a list of Outlinks
    List<Outlink> outlinks = new ArrayList<Outlink>();
    for (int i = 0; i < urls.size(); i++) {
      String rawUrl = urls.get(i);
      try {
        URL url = URLUtil.resolveURL(base, rawUrl);
        outlinks.add(new Outlink(url.toString(), ""));
      } catch (MalformedURLException e) {
        LOG.debug("failed to resolve url (base: {}, path: {})", base, rawUrl);
      }
    }

    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, "",
        outlinks.toArray(new Outlink[0]), content.getMetadata(), new Metadata());
    return ParseResult.createParseResult(content.getUrl(), new ParseImpl("", parseData));
  }

  private void collectStyleDeclarationOutlinks(CSSStyleDeclaration style, List<String> urls) {
    for (int i = 0; i < style.getLength(); i++) {
      String property = style.item(i);
      CSSValue value = style.getPropertyCSSValue(property);
      switch (value.getCssValueType()) {
        case CSSValue.CSS_PRIMITIVE_VALUE:
          collectPropertyPrimitiveValueOutlinks((CSSPrimitiveValue)value, urls);
          break;
        case CSSValue.CSS_VALUE_LIST:
          collectPropertyValueListOutlinks((CSSValueList)value, urls);
          break;
        default: break;
      }
    }
  }

  private void collectPropertyPrimitiveValueOutlinks(CSSPrimitiveValue value, List<String> urls) {
    if (value.getPrimitiveType() == CSSPrimitiveValue.CSS_URI) {
      String uri = value.getStringValue();
      // ignore "data" URIs (http://tools.ietf.org/html/rfc2397)
      if (!uri.startsWith("data:")) {
        urls.add(uri);
      }
    }
  }

  private void collectPropertyValueListOutlinks(CSSValueList values, List<String> urls) {
    for (int i = 0; i < values.getLength(); i++) {
      CSSValue value = values.item(i);

      switch (value.getCssValueType()) {
        case CSSValue.CSS_PRIMITIVE_VALUE:
          collectPropertyPrimitiveValueOutlinks((CSSPrimitiveValue)value, urls);
          break;
        case CSSValue.CSS_VALUE_LIST:
          // ignore nested value lists
          break;
        default: break;
      }
    }

  }

  @Override
  public Configuration getConf() {
    return this.config;
  }

  @Override
  public void setConf(Configuration conf) {
    this.config = conf;
  }

  public static void main(String[] args) throws Exception {
    String name = args[0];
    String url = "file:" + name;
    File file = new File(name);
    byte[] bytes = new byte[(int) file.length()];
    DataInputStream in = new DataInputStream(new FileInputStream(file));
    in.readFully(bytes);
    Configuration conf = NutchConfiguration.create();
    CssParser parser = new CssParser();
    parser.setConf(conf);
    Parse parse = parser.getParse(
        new Content(url, url, bytes, "text/css", new Metadata(), conf)).get(
        url);
    System.out.println("data: " + parse.getData());
    System.out.println("text: " + parse.getText());
  }
}

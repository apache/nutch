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
package org.apache.nutch.parse.tika;

import java.lang.invoke.MethodHandles;
import java.io.ByteArrayInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilters;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.OutlinkExtractor;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.protocol.Content;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.html.BoilerpipeContentHandler;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.sax.Link;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Element;
import org.xml.sax.ContentHandler;

/**
 * Wrapper for Tika parsers. Mimics the HTMLParser but using the XHTML
 * representation returned by Tika as SAX events
 ***/

public class TikaParser implements org.apache.nutch.parse.Parser {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Configuration conf;
  private TikaConfig tikaConfig = null;
  private DOMContentUtils utils;
  private HtmlParseFilters htmlParseFilters;
  private String cachingPolicy;
  private HtmlMapper HTMLMapper;
  private boolean upperCaseElementNames = true;

  @SuppressWarnings("deprecation")
  public ParseResult getParse(Content content) {
    String mimeType = content.getContentType();
    
    boolean useBoilerpipe = getConf().get("tika.extractor", "none").equals("boilerpipe");
    String boilerpipeExtractorName = getConf().get("tika.extractor.boilerpipe.algorithm", "ArticleExtractor");

    URL base;
    try {
      base = new URL(content.getBaseUrl());
    } catch (MalformedURLException e) {
      return new ParseStatus(e)
          .getEmptyParseResult(content.getUrl(), getConf());
    }

    // get the right parser using the mime type as a clue
    Parser parser = tikaConfig.getParser(MediaType.parse(mimeType));
    byte[] raw = content.getContent();

    if (parser == null) {
      String message = "Can't retrieve Tika parser for mime-type " + mimeType;
      LOG.error(message);
      return new ParseStatus(ParseStatus.FAILED, message).getEmptyParseResult(
          content.getUrl(), getConf());
    }

    LOG.debug("Using Tika parser " + parser.getClass().getName()
        + " for mime-type " + mimeType);

    Metadata tikamd = new Metadata();

    HTMLDocumentImpl doc = new HTMLDocumentImpl();
    doc.setErrorChecking(false);
    DocumentFragment root = doc.createDocumentFragment();

    ContentHandler domHandler;
    
    // Check whether to use Tika's BoilerplateContentHandler
    if (useBoilerpipe) {
      BoilerpipeContentHandler bpHandler = new BoilerpipeContentHandler((ContentHandler)new DOMBuilder(doc, root),
      BoilerpipeExtractorRepository.getExtractor(boilerpipeExtractorName));
      bpHandler.setIncludeMarkup(true);
      domHandler = (ContentHandler)bpHandler;
    } else {
      DOMBuilder domBuilder = new DOMBuilder(doc, root);
      domBuilder.setUpperCaseElementNames(upperCaseElementNames);
      domBuilder.setDefaultNamespaceURI(XHTMLContentHandler.XHTML);
      domHandler = (ContentHandler)domBuilder;
    }

    LinkContentHandler linkContentHandler = new LinkContentHandler();

    ParseContext context = new ParseContext();
    TeeContentHandler teeContentHandler = new TeeContentHandler(domHandler, linkContentHandler);
    
    if (HTMLMapper != null)
      context.set(HtmlMapper.class, HTMLMapper);
    tikamd.set(Metadata.CONTENT_TYPE, mimeType);
    try {
      parser.parse(new ByteArrayInputStream(raw), (ContentHandler)teeContentHandler, tikamd, context);
    } catch (Exception e) {
      LOG.error("Error parsing " + content.getUrl(), e);
      return new ParseStatus(ParseStatus.FAILED, e.getMessage())
          .getEmptyParseResult(content.getUrl(), getConf());
    }

    HTMLMetaTags metaTags = new HTMLMetaTags();
    String text = "";
    String title = "";
    Outlink[] outlinks = new Outlink[0];
    org.apache.nutch.metadata.Metadata nutchMetadata = new org.apache.nutch.metadata.Metadata();

    // we have converted the sax events generated by Tika into a DOM object
    // so we can now use the usual HTML resources from Nutch
    // get meta directives
    HTMLMetaProcessor.getMetaTags(metaTags, root, base);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Meta tags for " + base + ": " + metaTags.toString());
    }

    // check meta directives
    if (!metaTags.getNoIndex()) { // okay to index
      StringBuffer sb = new StringBuffer();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Getting text...");
      }
      utils.getText(sb, root); // extract text
      text = sb.toString();
      sb.setLength(0);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Getting title...");
      }
      utils.getTitle(sb, root); // extract title
      title = sb.toString().trim();
    }

    if (!metaTags.getNoFollow()) { // okay to follow links
      ArrayList<Outlink> l = new ArrayList<Outlink>(); // extract outlinks
      URL baseTag = base;
      String baseTagHref = tikamd.get("Content-Location");
      if (baseTagHref != null) {
        try {
          baseTag = new URL(base, baseTagHref);
        } catch (MalformedURLException e) {
          LOG.trace("Invalid <base href=\"{}\">", baseTagHref);
        }
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Getting links (base URL = {}) ...", baseTag);
      }
      
      // pre-1233 outlink extraction
      //utils.getOutlinks(baseTag != null ? baseTag : base, l, root);
      // Get outlinks from Tika
      List<Link> tikaExtractedOutlinks = linkContentHandler.getLinks();
      utils.getOutlinks(baseTag, l, tikaExtractedOutlinks);
      outlinks = l.toArray(new Outlink[l.size()]);
      if (LOG.isTraceEnabled()) {
        LOG.trace("found " + outlinks.length + " outlinks in "
            + content.getUrl());
      }
    }

    // populate Nutch metadata with Tika metadata
    String[] TikaMDNames = tikamd.names();
    for (String tikaMDName : TikaMDNames) {
      if (tikaMDName.equalsIgnoreCase(Metadata.TITLE))
        continue;
      String[] values = tikamd.getValues(tikaMDName);
      for (String v : values)
        nutchMetadata.add(tikaMDName, v);
    }

    // no outlinks? try OutlinkExtractor e.g works for mime types where no
    // explicit markup for anchors

    if (outlinks.length == 0) {
      outlinks = OutlinkExtractor.getOutlinks(text, getConf());
    }

    ParseStatus status = new ParseStatus(ParseStatus.SUCCESS);
    if (metaTags.getRefresh()) {
      status.setMinorCode(ParseStatus.SUCCESS_REDIRECT);
      status.setArgs(new String[] { metaTags.getRefreshHref().toString(),
          Integer.toString(metaTags.getRefreshTime()) });
    }
    ParseData parseData = new ParseData(status, title, outlinks,
        content.getMetadata(), nutchMetadata);
    ParseResult parseResult = ParseResult.createParseResult(content.getUrl(),
        new ParseImpl(text, parseData));

    // run filters on parse
    ParseResult filteredParse = this.htmlParseFilters.filter(content,
        parseResult, metaTags, root);
    if (metaTags.getNoCache()) { // not okay to cache
      for (Map.Entry<org.apache.hadoop.io.Text, Parse> entry : filteredParse)
        entry.getValue().getData().getParseMeta()
            .set(Nutch.CACHING_FORBIDDEN_KEY, cachingPolicy);
    }
    return filteredParse;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.tikaConfig = null;

    // do we want a custom Tika configuration file
    // deprecated since Tika 0.7 which is based on
    // a service provider based configuration
    String customConfFile = conf.get("tika.config.file");
    if (customConfFile != null) {
      try {
        // see if a Tika config file can be found in the job file
        URL customTikaConfig = conf.getResource(customConfFile);
        if (customTikaConfig != null)
          tikaConfig = new TikaConfig(customTikaConfig, this.getClass().getClassLoader());
      } catch (Exception e1) {
        String message = "Problem loading custom Tika configuration from "
            + customConfFile;
        LOG.error(message, e1);
      }
    } else {
      try {
        tikaConfig = new TikaConfig(this.getClass().getClassLoader());
      } catch (Exception e2) {
        String message = "Problem loading default Tika configuration";
        LOG.error(message, e2);
      }
    }

    // use a custom htmlmapper
    String htmlmapperClassName = conf.get("tika.htmlmapper.classname");
    if (StringUtils.isNotBlank(htmlmapperClassName)) {
      try {
        Class HTMLMapperClass = Class.forName(htmlmapperClassName);
        boolean interfaceOK = HtmlMapper.class
            .isAssignableFrom(HTMLMapperClass);
        if (!interfaceOK) {
          throw new RuntimeException("Class " + htmlmapperClassName
              + " does not implement HtmlMapper");
        }
        HTMLMapper = (HtmlMapper) HTMLMapperClass.newInstance();
      } catch (Exception e) {
        LOG.error("Can't generate instance for class " + htmlmapperClassName);
        throw new RuntimeException("Can't generate instance for class "
            + htmlmapperClassName);
      }
    }

    this.htmlParseFilters = new HtmlParseFilters(getConf());
    this.utils = new DOMContentUtils(conf);
    this.cachingPolicy = getConf().get("parser.caching.forbidden.policy",
        Nutch.CACHING_FORBIDDEN_CONTENT);
    this.upperCaseElementNames = getConf().getBoolean(
        "tika.uppercase.element.names", true);
  }

  public Configuration getConf() {
    return this.conf;
  }

}

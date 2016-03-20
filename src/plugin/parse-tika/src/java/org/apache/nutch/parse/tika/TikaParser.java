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

import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.*;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.html.BoilerpipeContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.xml.sax.ContentHandler;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

/**
 * Wrapper for Tika parsers. Mimics the HTMLParser but using the XHTML
 * representation returned by Tika as SAX events
 ***/

public class TikaParser implements org.apache.nutch.parse.Parser {

  public static final Logger LOG = LoggerFactory.getLogger(TikaParser.class);

  private static Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.BASE_URL);
    FIELDS.add(WebPage.Field.CONTENT_TYPE);
  }

  private Configuration conf;
  private TikaConfig tikaConfig = null;
  private DOMContentUtils utils;
  private ParseFilters htmlParseFilters;
  private String cachingPolicy;

  private HtmlMapper HTMLMapper;

  @Override
  public Parse getParse(String url, WebPage page) {

    boolean useBoilerpipe = getConf().getBoolean("tika.boilerpipe", false); 
    String boilerpipeExtractorName = getConf().get("tika.boilerpipe.extractor", "ArticleExtractor");

    String baseUrl = TableUtil.toString(page.getBaseUrl());
    URL base;
    try {
      base = new URL(baseUrl);
    } catch (MalformedURLException e) {
      return ParseStatusUtils.getEmptyParse(e, getConf());
    }

    // get the right parser using the mime type as a clue
    String mimeType = page.getContentType().toString();
    CompositeParser compositeParser = (CompositeParser) tikaConfig.getParser();
    Parser parser = compositeParser.getParsers().get(MediaType.parse(mimeType));
    ByteBuffer raw = page.getContent();

    if (parser == null) {
      String message = "Can't retrieve Tika parser for mime-type " + mimeType;
      LOG.error(message);
      return ParseStatusUtils.getEmptyParse(ParseStatusCodes.FAILED_EXCEPTION,
          message, getConf());
    }

    LOG.debug("Using Tika parser {} for mime-type {}.", parser.getClass().getName(), mimeType);

    Metadata tikamd = new Metadata();

    HTMLDocumentImpl doc = new HTMLDocumentImpl();
    doc.setErrorChecking(false);
    DocumentFragment root = doc.createDocumentFragment();
    ContentHandler domHandler;
    // Check whether to use Tika's BoilerplateContentHandler
    if (useBoilerpipe) {
        LOG.debug("Using Tikas's Boilerpipe with Extractor: {}.", boilerpipeExtractorName);
        BoilerpipeContentHandler bpHandler = new BoilerpipeContentHandler((ContentHandler)new DOMBuilder(doc, root), BoilerpipeExtractorRepository.getExtractor(boilerpipeExtractorName));
        bpHandler.setIncludeMarkup(true);
        domHandler = (ContentHandler)bpHandler;
    } else {
        domHandler = new DOMBuilder(doc, root);
    }
    
    ParseContext context = new ParseContext();
    if (HTMLMapper != null)
      context.set(HtmlMapper.class, HTMLMapper);
    // to add once available in Tika
    // context.set(HtmlMapper.class, IdentityHtmlMapper.INSTANCE);
    tikamd.set(Metadata.CONTENT_TYPE, mimeType);
    try {
      parser.parse(new ByteArrayInputStream(raw.array(), raw.arrayOffset()
          + raw.position(), raw.remaining()), (ContentHandler)domHandler, tikamd, context);
    } catch (Exception e) {
      LOG.error("Error parsing {}.", url, e);
      return ParseStatusUtils.getEmptyParse(e, getConf());
    }

    HTMLMetaTags metaTags = new HTMLMetaTags();
    String text = "";
    String title = "";
    Outlink[] outlinks = new Outlink[0];

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

    // Parse again without boilerpipe to get all outlinks
    // TODO avoid this second parsing
    if (useBoilerpipe) {
        root = doc.createDocumentFragment();
        domHandler = new DOMBuilder(doc, root);
        try {
            parser.parse(new ByteArrayInputStream(raw.array(), raw.arrayOffset() + raw.position(), raw.remaining()), (ContentHandler)domHandler, tikamd, context);
        } catch (Exception e) {
	    LOG.error("Error parsing {}.", url, e);
	    return ParseStatusUtils.getEmptyParse(e, getConf());
        }
    }
    

    if (!metaTags.getNoFollow()) { // okay to follow links
      ArrayList<Outlink> l = new ArrayList<Outlink>(); // extract outlinks
      URL baseTag = utils.getBase(root);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Getting links...");
      }
      utils.getOutlinks(baseTag != null ? baseTag : base, l, root);
      outlinks = l.toArray(new Outlink[l.size()]);
      if (LOG.isTraceEnabled()) {
        LOG.trace("found " + outlinks.length + " outlinks in " + base);
      }
    }

    // populate Nutch metadata with Tika metadata
    String[] TikaMDNames = tikamd.names();
    for (String tikaMDName : TikaMDNames) {
      if (tikaMDName.equalsIgnoreCase(TikaCoreProperties.TITLE.toString()))
        continue;
      // TODO what if multivalued?
      page.getMetadata().put(new Utf8(tikaMDName),
          ByteBuffer.wrap(Bytes.toBytes(tikamd.get(tikaMDName))));
    }

    // no outlinks? try OutlinkExtractor e.g works for mime types where no
    // explicit markup for anchors

    if (outlinks.length == 0) {
      outlinks = OutlinkExtractor.getOutlinks(text, getConf());
    }

    ParseStatus status = ParseStatusUtils.STATUS_SUCCESS;
    if (metaTags.getRefresh()) {
      status.setMinorCode((int) ParseStatusCodes.SUCCESS_REDIRECT);
      status.getArgs().add(new Utf8(metaTags.getRefreshHref().toString()));
      status.getArgs().add(
          new Utf8(Integer.toString(metaTags.getRefreshTime())));
    }

    Parse parse = new Parse(text, title, outlinks, status);
    parse = htmlParseFilters.filter(url, page, parse, metaTags, root);

    if (metaTags.getNoCache()) { // not okay to cache
      page.getMetadata().put(new Utf8(Nutch.CACHING_FORBIDDEN_KEY),
          ByteBuffer.wrap(Bytes.toBytes(cachingPolicy)));
    }

    return parse;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.tikaConfig = null;

    try {
      tikaConfig = new TikaConfig(this.getClass().getClassLoader());
    } catch (Exception e2) {
      String message = "Problem loading default Tika configuration";
      LOG.error(message, e2);
      throw new RuntimeException(e2);
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

    this.htmlParseFilters = new ParseFilters(getConf());
    this.utils = new DOMContentUtils(conf);
    this.cachingPolicy = getConf().get("parser.caching.forbidden.policy",
        Nutch.CACHING_FORBIDDEN_CONTENT);
  }

  public TikaConfig getTikaConfig() {
    return this.tikaConfig;
  }

  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public Collection<Field> getFields() {
    return FIELDS;
  }

  // main class used for debuggin
  public static void main(String[] args) throws Exception {
    String name = args[0];
    String url = "file:" + name;
    File file = new File(name);
    byte[] bytes = new byte[(int) file.length()];
    @SuppressWarnings("resource")
    DataInputStream in = new DataInputStream(new FileInputStream(file));
    in.readFully(bytes);
    Configuration conf = NutchConfiguration.create();
    // TikaParser parser = new TikaParser();
    // parser.setConf(conf);
    WebPage page = WebPage.newBuilder().build();
    page.setBaseUrl(new Utf8(url));
    page.setContent(ByteBuffer.wrap(bytes));
    MimeUtil mimeutil = new MimeUtil(conf);
    String mtype = mimeutil.getMimeType(file);
    page.setContentType(new Utf8(mtype));
    // Parse parse = parser.getParse(url, page);

    Parse parse = new ParseUtil(conf).parse(url, page);

    System.out.println("content type: " + mtype);
    System.out.println("title: " + parse.getTitle());
    System.out.println("text: " + parse.getText());
    System.out.println("outlinks: " + Arrays.toString(parse.getOutlinks()));
  }
}

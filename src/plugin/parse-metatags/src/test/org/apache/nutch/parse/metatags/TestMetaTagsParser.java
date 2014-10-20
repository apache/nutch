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

package org.apache.nutch.parse.metatags;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.metatags.MetaTagsParser;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.NutchConfiguration;
import org.cyberneko.html.parsers.DOMFragmentParser;
import org.junit.Test;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Map;

public class TestMetaTagsParser {

  private String fileSeparator = System.getProperty("file.separator");
  private String sampleDir = System.getProperty("test.data", ".");
  private String sampleFile = "testMetatags.html";
  private String description = "This is a test of description";
  private String keywords = "This is a test of keywords";

  /**
   * 
   *
   * @param fileName
   *          This variable set test file.
   * @param useUtil
   *          If value is True method use ParseUtil
   * @return If successfully document parsed, it return metatags
   */
  public Map<CharSequence, ByteBuffer> parseMetaTags(String fileName, boolean useUtil) {
    try {
      Configuration conf = NutchConfiguration.create();
      String urlString = "file:" + sampleDir + fileSeparator + fileName;

      File file = new File(sampleDir + fileSeparator + fileName);
      byte[] bytes = new byte[(int) file.length()];
      DataInputStream in = new DataInputStream(new FileInputStream(file));
      in.readFully(bytes);
      in.close();

      WebPage page = WebPage.newBuilder().build();
      page.setBaseUrl(new Utf8(urlString));
      page.setContent(ByteBuffer.wrap(bytes));
      page.setContentType(new Utf8("text/html"));

      if (useUtil) {
        ParseUtil parser = new ParseUtil(conf);
        parser.parse(urlString, page);
      } else {
        DocumentFragment node = getDOMDocument(bytes);
        HTMLMetaTags metaTags = new HTMLMetaTags();
        URL base = null;
        try {
          base = new URL(urlString);
        } catch (MalformedURLException e) {
          e.printStackTrace();
        }
        // get meta directives
        getMetaTags(metaTags, node, base);

        MetaTagsParser mtp = new MetaTagsParser();
        mtp.setConf(conf);
        mtp.filter(urlString, page, new Parse(), metaTags, node);
      }

      return page.getMetadata();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
      return null;
    }
  }

  public static final void getMetaTagsHelper(HTMLMetaTags metaTags, Node node,
      URL currURL) {

    if (node.getNodeType() == Node.ELEMENT_NODE) {

      if ("body".equalsIgnoreCase(node.getNodeName())) {
        // META tags should not be under body
        return;
      }

      if ("meta".equalsIgnoreCase(node.getNodeName())) {
        NamedNodeMap attrs = node.getAttributes();
        Node nameNode = null;
        Node equivNode = null;
        Node contentNode = null;
        // Retrieves name, http-equiv and content attribues
        for (int i = 0; i < attrs.getLength(); i++) {
          Node attr = attrs.item(i);
          String attrName = attr.getNodeName().toLowerCase();
          if (attrName.equals("name")) {
            nameNode = attr;
          } else if (attrName.equals("http-equiv")) {
            equivNode = attr;
          } else if (attrName.equals("content")) {
            contentNode = attr;
          }
        }
        if (nameNode != null) {
          if (contentNode != null) {
            String name = nameNode.getNodeValue().toLowerCase();
            metaTags.getGeneralTags().add(name, contentNode.getNodeValue());
          }
        }

        if (equivNode != null) {
          if (contentNode != null) {
            String name = equivNode.getNodeValue().toLowerCase();
            String content = contentNode.getNodeValue();
            metaTags.getHttpEquivTags().setProperty(name, content);
          }
        }
      }
    }

    NodeList children = node.getChildNodes();
    if (children != null) {
      int len = children.getLength();
      for (int i = 0; i < len; i++) {
        getMetaTagsHelper(metaTags, children.item(i), currURL);
      }
    }
  }

  public static final void getMetaTags(HTMLMetaTags metaTags, Node node,
      URL currURL) {

    metaTags.reset();
    getMetaTagsHelper(metaTags, node, currURL);
  }

  private static DocumentFragment getDOMDocument(byte[] content)
      throws IOException, SAXException {
    InputSource input = new InputSource(new ByteArrayInputStream(content));
    input.setEncoding("utf-8");
    DOMFragmentParser parser = new DOMFragmentParser();
    DocumentFragment node = new HTMLDocumentImpl().createDocumentFragment();
    parser.parse(input, node);
    return node;
  }

  /**
   * This test use parse-html with other parse filters.
   */
  @Test
  public void testMetaTagsParserWithConf() {
    // check that we get the same values
    Map<CharSequence, ByteBuffer> meta = parseMetaTags(sampleFile, true);

    assertEquals(description,
        getMeta(meta, MetaTagsParser.PARSE_META_PREFIX + "description"));
    assertEquals(keywords,
        getMeta(meta, MetaTagsParser.PARSE_META_PREFIX + "keywords"));
  }

  /**
   * This test generate custom DOM tree without parse-html for testing just
   * parse-metatags.
   */
  @Test
  public void testFilter() {
    // check that we get the same values
    Map<CharSequence, ByteBuffer> meta = parseMetaTags(sampleFile, false);

    assertEquals(description,
        getMeta(meta, MetaTagsParser.PARSE_META_PREFIX + "description"));
    assertEquals(keywords,
        getMeta(meta, MetaTagsParser.PARSE_META_PREFIX + "keywords"));
  }

  private String getMeta(Map<CharSequence, ByteBuffer> meta, String name) {
    ByteBuffer raw = meta.get(new Utf8(name));
    return Bytes.toString(raw);
  }

}

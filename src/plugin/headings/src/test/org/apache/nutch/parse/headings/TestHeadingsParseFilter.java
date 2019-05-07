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
package org.apache.nutch.parse.headings;

import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.*;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.w3c.dom.DocumentFragment;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.cyberneko.html.parsers.DOMFragmentParser;
import org.junit.Assert;
import org.junit.Test;

public class TestHeadingsParseFilter {
  private static Configuration conf = NutchConfiguration.create();

  @Test
  public void testExtractHeadingFromNestedNodes()
      throws IOException, SAXException {

    conf.setStrings("headings", "h1", "h2");
    HtmlParseFilter filter = new HeadingsParseFilter();
    filter.setConf(conf);

    Content content = new Content("http://www.foo.com/", "http://www.foo.com/",
        "".getBytes("UTF8"), "text/html; charset=UTF-8", new Metadata(), conf);
    ParseImpl parse = new ParseImpl("foo bar", new ParseData());
    ParseResult parseResult = ParseResult
        .createParseResult("http://www.foo.com/", parse);
    HTMLMetaTags metaTags = new HTMLMetaTags();
    DOMFragmentParser parser = new DOMFragmentParser();
    DocumentFragment node = new HTMLDocumentImpl().createDocumentFragment();

    parser.parse(new InputSource(new ByteArrayInputStream(
        ("<html><head><title>test header with span element</title></head><body><h1>header with <span>span element</span></h1></body></html>")
            .getBytes())), node);

    parseResult = filter.filter(content, parseResult, metaTags, node);

    Assert.assertEquals(
        "The h1 tag must include the content of the inner span node",
        "header with span element",
        parseResult.get(content.getUrl()).getData().getParseMeta().get("h1"));
  }
}

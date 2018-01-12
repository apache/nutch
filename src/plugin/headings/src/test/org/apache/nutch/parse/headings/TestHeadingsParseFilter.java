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

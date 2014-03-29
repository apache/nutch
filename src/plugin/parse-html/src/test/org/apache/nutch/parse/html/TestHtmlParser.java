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

package org.apache.nutch.parse.html;

import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.html.HtmlParser;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHtmlParser {

  public static final Logger LOG = LoggerFactory.getLogger(TestHtmlParser.class);

  private static final String encodingTestKeywords = 
      "français, español, русский язык, čeština, ελληνικά";
  private static final String encodingTestBody =
      "<ul>\n  <li>français\n  <li>español\n  <li>русский язык\n  <li>čeština\n  <li>ελληνικά\n</ul>";
  private static final String encodingTestContent =
      "<title>" + encodingTestKeywords + "</title>\n"
          + "<meta name=\"keywords\" content=\"" + encodingTestKeywords + "</meta>\n"
          + "</head>\n<body>" + encodingTestBody + "</body>\n</html>";

  private static String[][] encodingTestPages= {
    { 
      "HTML4, utf-8, meta http-equiv, no quotes",
      "utf-8",
      "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" "
          + "\"http://www.w3.org/TR/html4/loose.dtd\">\n"
          + "<html>\n<head>\n"
          + "<meta http-equiv=Content-Type content=\"text/html; charset=utf-8\" />"
          + encodingTestContent
    },
    { 
      "HTML4, utf-8, meta http-equiv, single quotes",
      "utf-8",
      "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" "
          + "\"http://www.w3.org/TR/html4/loose.dtd\">\n"
          + "<html>\n<head>\n"
          + "<meta http-equiv='Content-Type' content='text/html; charset=utf-8' />"
          + encodingTestContent
    },
    { 
      "XHTML, utf-8, meta http-equiv, double quotes",
      "utf-8",
      "<?xml version=\"1.0\"?>\n<html xmlns=\"http://www.w3.org/1999/xhtml\">"
          + "<html>\n<head>\n"
          + "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
          + encodingTestContent
    },
    { 
      "HTML5, utf-8, meta charset",
      "utf-8",
      "<!DOCTYPE html>\n<html>\n<head>\n"
          + "<meta charset=\"utf-8\">"
          + encodingTestContent
    },
    { 
      "HTML5, utf-8, BOM",
      "utf-8",
      "\ufeff<!DOCTYPE html>\n<html>\n<head>\n"
          + encodingTestContent
    },
    { 
      "HTML5, utf-16, BOM",
      "utf-16",
      "\ufeff<!DOCTYPE html>\n<html>\n<head>\n"
          + encodingTestContent
    }
  };

  private Configuration conf;
  private Parser parser;

  public TestHtmlParser() { 
    conf = NutchConfiguration.create();
    parser = new HtmlParser();
    parser.setConf(conf);
  }

  protected Parse parse(byte[] contentBytes) {
    String dummyUrl = "http://dummy.url/";
    return parser.getParse(
        new Content(dummyUrl, dummyUrl, contentBytes, "text/html", new Metadata(),
            conf)).get(dummyUrl);
  }

  @Test
  public void testEncodingDetection() {
    for (String[] testPage : encodingTestPages) {
      String name = testPage[0];
      Charset charset = Charset.forName(testPage[1]);
      byte[] contentBytes = testPage[2].getBytes(charset);
      Parse parse = parse(contentBytes);
      String text = parse.getText();
      String title = parse.getData().getTitle();
      String keywords = parse.getData().getMeta("keywords");
      LOG.info(name);
      LOG.info("title:\t" + title);
      LOG.info("keywords:\t" + keywords);
      LOG.info("text:\t" + text);
      Assert.assertEquals("Title not extracted properly (" + name + ")",
          encodingTestKeywords, title);
      for (String keyword : encodingTestKeywords.split(",\\s*")) {
        Assert.assertTrue(keyword + " not found in text (" + name + ")",
            text.contains(keyword));
      }
      if (keywords != null) {
        Assert.assertEquals("Keywords not extracted properly (" + name + ")",
            encodingTestKeywords, keywords);
      }
    }
  }

}

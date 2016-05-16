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

import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.parse.*;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCssParser {
  public static final Logger LOG = LoggerFactory.getLogger(TestCssParser.class);

  private static final String fullCss =
    "@import url(\"fineprint.css\") print;\n" +
    "@import url(\"bluish.css\") projection, tv;\n" +
    "@import 'custom.css';\n" +
    "@import url(\"chrome://communicator/skin/\");\n" +
    "@import \"common.css\" screen, projection;\n" +
    "@import url('landscape.css') screen and (orientation:landscape);\n" +
    "@font-face {\n" +
    "  font-family: \"Bitstream Vera Serif Bold\";\n" +
    "  src: url(\"https://mdn.mozillademos.org/files/2468/VeraSeBd.ttf\");\n" +
    "}\n" +
    "@font-face {\n" +
    "  font-family: MyHelvetica;\n" +
    "  src: local(\"Helvetica Neue Bold\"),\n" +
    "       local(\"HelveticaNeue-Bold\"),\n" +
    "       url(MgOpenModernaBold.ttf);\n" +
    "  font-weight: bold;\n" +
    "}\n" +
    "@font-face {\n" +
    "  font-family: MyHelvetica;\n" +
    "  src: url('Fixedys.ttf'),\n" +
    "       url(\"FixedysOpen.ttf\"),\n" +
    "       url(OpenFixedys.ttf);\n" +
    "  font-weight: bold;\n" +
    "}\n" +
    ".topbanner { background: url(\"topbanner.png\") #00D no-repeat fixed; }\n" +
    ".footer { background: url(./resources/footer.png); }\n" +
    "ul { list-style: square url(http://www.example.com/redball.png) }\n" +
    "div#header {\n" +
    "    background-image: url('images/header-background.jpg');\n" +
    "}\n" +
    "html {\n" +
    "  cursor: url(../cursors/cursor1.cur) 2 2, url(../images/cursor2.png),\n" +
    "   url('../images/cursor3.gif') 4 12,default;\n" +
    "}\n" +
    "li {\n" +
    "  background:\n" +
    "    url(data:image/gif;base64,FEDCBA9876543210)\n" +
    "    no-repeat\n" +
    "    left center;\n" +
    "  padding: 5px 0 5px 25px;\n" +
    "}";

  private Configuration conf;
  private Parser parser;

  public TestCssParser() {
    conf = NutchConfiguration.create();
    parser = new CssParser();
    parser.setConf(conf);
  }

  @Test
  public void testLinkExtraction() throws MalformedURLException {
    byte[] contentBytes = fullCss.getBytes(StandardCharsets.UTF_8);
    String dummyBase = "http://dummy.url/";
    String dummyUrl = dummyBase + "style/dummy.css";
    Content content = new Content(dummyUrl, dummyUrl, contentBytes, "text/css",
        new Metadata(), conf);
    Parse parse = parser.getParse(content).get(dummyUrl);
    Outlink[] parsedOutlinks = parse.getData().getOutlinks();

    String anchor = "";
    Outlink[] expectedOutlinks = {
      new Outlink(dummyBase + "style/fineprint.css", anchor),
      new Outlink(dummyBase + "style/bluish.css", anchor),
      new Outlink(dummyBase + "style/custom.css", anchor),
      new Outlink(dummyBase + "style/common.css", anchor),
      new Outlink(dummyBase + "style/landscape.css", anchor),
      new Outlink("https://mdn.mozillademos.org/files/2468/VeraSeBd.ttf", anchor),
      new Outlink(dummyBase + "style/MgOpenModernaBold.ttf", anchor),
      new Outlink(dummyBase + "style/Fixedys.ttf", anchor),
      new Outlink(dummyBase + "style/FixedysOpen.ttf", anchor),
      new Outlink(dummyBase + "style/OpenFixedys.ttf", anchor),
      new Outlink(dummyBase + "style/topbanner.png", anchor),
      new Outlink(dummyBase + "style/resources/footer.png", anchor),
      new Outlink("http://www.example.com/redball.png", anchor),
      new Outlink(dummyBase + "style/images/header-background.jpg", anchor),
      new Outlink(dummyBase + "cursors/cursor1.cur", anchor),
      new Outlink(dummyBase + "images/cursor2.png", anchor),
      new Outlink(dummyBase + "images/cursor3.gif", anchor)
    };

    Assert.assertArrayEquals("Parsed Outlinks do not match expected", expectedOutlinks, parsedOutlinks);
  }
}

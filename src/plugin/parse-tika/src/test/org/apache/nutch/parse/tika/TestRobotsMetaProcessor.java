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

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.DocumentFragment;

/** Unit tests for HTMLMetaProcessor. */
public class TestRobotsMetaProcessor {

  /*
   * 
   * some sample tags:
   * 
   * <meta name="robots" content="index,follow"> <meta name="robots"
   * content="noindex,follow"> <meta name="robots" content="index,nofollow">
   * <meta name="robots" content="noindex,nofollow">
   * 
   * <META HTTP-EQUIV="Pragma" CONTENT="no-cache">
   */

  public static String[] tests = {
      "<html><head><title>test page</title>"
          + "<META NAME=\"ROBOTS\" CONTENT=\"NONE\"> "
          + "<META HTTP-EQUIV=\"PRAGMA\" CONTENT=\"NO-CACHE\"> "
          + "</head><body>" + " some text" + "</body></html>",

      "<html><head><title>test page</title>"
          + "<meta name=\"robots\" content=\"all\"> "
          + "<meta http-equiv=\"pragma\" content=\"no-cache\"> "
          + "</head><body>" + " some text" + "</body></html>",

      "<html><head><title>test page</title>"
          + "<MeTa NaMe=\"RoBoTs\" CoNtEnT=\"nOnE\"> "
          + "<MeTa HtTp-EqUiV=\"pRaGmA\" cOnTeNt=\"No-CaChE\"> "
          + "</head><body>" + " some text" + "</body></html>",

      "<html><head><title>test page</title>"
          + "<meta name=\"robots\" content=\"none\"> " + "</head><body>"
          + " some text" + "</body></html>",

      "<html><head><title>test page</title>"
          + "<meta name=\"robots\" content=\"noindex,nofollow\"> "
          + "</head><body>" + " some text" + "</body></html>",

      "<html><head><title>test page</title>"
          + "<meta name=\"robots\" content=\"noindex,follow\"> "
          + "</head><body>" + " some text" + "</body></html>",

      "<html><head><title>test page</title>"
          + "<meta name=\"robots\" content=\"index,nofollow\"> "
          + "</head><body>" + " some text" + "</body></html>",

      "<html><head><title>test page</title>"
          + "<meta name=\"robots\" content=\"index,follow\"> "
          + "<base href=\"http://www.nutch.org/\">" + "</head><body>"
          + " some text" + "</body></html>",

      "<html><head><title>test page</title>" + "<meta name=\"robots\"> "
          + "<base href=\"http://www.nutch.org/base/\">" + "</head><body>"
          + " some text" + "</body></html>",

      "<html><head><title>Meta-refresh redirect</title>"
          + "<meta http-equiv=\"refresh\" content=\"0; url=http://example.com/\"></head><body> "
          + "Test meta-refresh redirect." + "</body></html>",
  };

  public static final boolean[][] answers = { //
      { true, true, true }, // NONE
      { false, false, true }, // all
      { true, true, true }, // nOnE
      { true, true, false }, // none
      { true, true, false }, // noindex,nofollow
      { true, false, false }, // noindex,follow
      { false, true, false }, // index,nofollow
      { false, false, false }, // index,follow
      { false, false, false }, // missing!
      { false, false, false }, // NUTCH-2589: test for meta-refresh redirects
  };

  private URL[][] currURLsAndAnswers;

  @Test
  public void testRobotsMetaProcessor() {
    Configuration conf = NutchConfiguration.create();
    TikaParser parser = new TikaParser();
    parser.setConf(conf);

    try {
      currURLsAndAnswers = new URL[][] {
          { new URL("http://www.nutch.org"), null },
          { new URL("http://www.nutch.org"), null },
          { new URL("http://www.nutch.org"), null },
          { new URL("http://www.nutch.org"), null },
          { new URL("http://www.nutch.org"), null },
          { new URL("http://www.nutch.org"), null },
          { new URL("http://www.nutch.org"), null },
          { new URL("http://www.nutch.org/foo/"),
              new URL("http://www.nutch.org/") },
          { new URL("http://www.nutch.org"),
              new URL("http://www.nutch.org/base/") },
          { new URL("http://www.nutch.org"), null } };
    } catch (Exception e) {
      Assert.assertTrue("couldn't make test URLs!", false);
    }

    for (int i = 0; i < tests.length; i++) {
      byte[] bytes = tests[i].getBytes();

      HTMLDocumentImpl doc = new HTMLDocumentImpl();
      doc.setErrorChecking(false);
      DocumentFragment root = doc.createDocumentFragment();
      String url = "http://www.nutch.org";
      Content content = new Content(url,
          url, bytes, "text/html", new Metadata(), conf);
      Parse parse = null;

      try {
        parse = parser.getParse(content, doc, root).get(url);
      } catch (Exception e) {
        e.printStackTrace();
      }

      HTMLMetaTags robotsMeta = new HTMLMetaTags();
      HTMLMetaProcessor.getMetaTags(robotsMeta, root, currURLsAndAnswers[i][0]);

      Assert.assertEquals("got noindex wrong on test " + i,
          answers[i][0], robotsMeta.getNoIndex());
      Assert.assertEquals("got nofollow wrong on test " + i,
          answers[i][1], robotsMeta.getNoFollow());
      Assert.assertEquals("got nocache wrong on test " + i,
          answers[i][2], robotsMeta.getNoCache());
      Assert
          .assertTrue(
              "got base href wrong on test " + i + " (got "
                  + robotsMeta.getBaseHref() + ")",
              ((robotsMeta.getBaseHref() == null) && (currURLsAndAnswers[i][1] == null))
                  || ((robotsMeta.getBaseHref() != null) && robotsMeta
                      .getBaseHref().equals(currURLsAndAnswers[i][1])));

      if (tests[i].contains("meta-refresh redirect")) {
        // test for NUTCH-2589
        URL metaRefreshUrl = robotsMeta.getRefreshHref();
        Assert.assertNotNull("failed to get meta-refresh redirect",
            metaRefreshUrl);
        Assert.assertEquals("failed to get meta-refresh redirect",
            "http://example.com/", metaRefreshUrl.toString());
        Assert.assertEquals(
            "failed to add meta-refresh redirect to parse status",
            "http://example.com/", parse.getData().getStatus().getArgs()[0]);
      }
    }
  }

}

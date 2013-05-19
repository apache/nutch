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

import org.apache.nutch.parse.HTMLMetaTags;

import java.io.ByteArrayInputStream;
import java.net.URL;

import org.cyberneko.html.parsers.*;
import org.xml.sax.*;
import org.w3c.dom.*;
import org.apache.html.dom.*;

import org.junit.Test;
import static org.junit.Assert.*;

/** Unit tests for HTMLMetaProcessor. */
public class TestRobotsMetaProcessor {

  /*

  some sample tags:

  <meta name="robots" content="index,follow">
  <meta name="robots" content="noindex,follow">
  <meta name="robots" content="index,nofollow">
  <meta name="robots" content="noindex,nofollow">

  <META HTTP-EQUIV="Pragma" CONTENT="no-cache">

  */


  public static String[] tests= 
  {
    "<html><head><title>test page</title>"
    + "<META NAME=\"ROBOTS\" CONTENT=\"NONE\"> "
    + "<META HTTP-EQUIV=\"PRAGMA\" CONTENT=\"NO-CACHE\"> "
    + "</head><body>"
    + " some text"
    + "</body></html>",

    "<html><head><title>test page</title>"
    + "<meta name=\"robots\" content=\"all\"> "
    + "<meta http-equiv=\"pragma\" content=\"no-cache\"> "
    + "</head><body>"
    + " some text"
    + "</body></html>",

    "<html><head><title>test page</title>"
    + "<MeTa NaMe=\"RoBoTs\" CoNtEnT=\"nOnE\"> "
    + "<MeTa HtTp-EqUiV=\"pRaGmA\" cOnTeNt=\"No-CaChE\"> "
    + "</head><body>"
    + " some text"
    + "</body></html>",

    "<html><head><title>test page</title>"
    + "<meta name=\"robots\" content=\"none\"> "
    + "</head><body>"
    + " some text"
    + "</body></html>",

    "<html><head><title>test page</title>"
    + "<meta name=\"robots\" content=\"noindex,nofollow\"> "
    + "</head><body>"
    + " some text"
    + "</body></html>",

    "<html><head><title>test page</title>"
    + "<meta name=\"robots\" content=\"noindex,follow\"> "
    + "</head><body>"
    + " some text"
    + "</body></html>",

    "<html><head><title>test page</title>"
    + "<meta name=\"robots\" content=\"index,nofollow\"> "
    + "</head><body>"
    + " some text"
    + "</body></html>",

    "<html><head><title>test page</title>"
    + "<meta name=\"robots\" content=\"index,follow\"> "
    + "<base href=\"http://www.nutch.org/\">"
    + "</head><body>"
    + " some text"
    + "</body></html>",

    "<html><head><title>test page</title>"
    + "<meta name=\"robots\"> "
    + "<base href=\"http://www.nutch.org/base/\">"
    + "</head><body>"
    + " some text"
    + "</body></html>",

  };

  public static final boolean[][] answers= {
    {true, true, true},     // NONE
    {false, false, true},   // all
    {true, true, true},     // nOnE
    {true, true, false},    // none
    {true, true, false},    // noindex,nofollow
    {true, false, false},   // noindex,follow
    {false, true, false},   // index,nofollow
    {false, false, false},  // index,follow
    {false, false, false},  // missing!
  };

  private URL[][] currURLsAndAnswers;

  @Test
  public void testRobotsMetaProcessor() {
    DOMFragmentParser parser= new DOMFragmentParser();;

    try { 
      currURLsAndAnswers= new URL[][] {
        {new URL("http://www.nutch.org"), null},
        {new URL("http://www.nutch.org"), null},
        {new URL("http://www.nutch.org"), null},
        {new URL("http://www.nutch.org"), null},
        {new URL("http://www.nutch.org"), null},
        {new URL("http://www.nutch.org"), null},
        {new URL("http://www.nutch.org"), null},
        {new URL("http://www.nutch.org/foo/"), 
         new URL("http://www.nutch.org/")},
        {new URL("http://www.nutch.org"), 
         new URL("http://www.nutch.org/base/")}
      };
    } catch (Exception e) {
      assertTrue("couldn't make test URLs!", false);
    }

    for (int i= 0; i < tests.length; i++) {
      byte[] bytes= tests[i].getBytes();

      DocumentFragment node = new HTMLDocumentImpl().createDocumentFragment();

      try {
        parser.parse(new InputSource(new ByteArrayInputStream(bytes)), node);
      } catch (Exception e) {
        e.printStackTrace();
      }

      HTMLMetaTags robotsMeta= new HTMLMetaTags();
      HTMLMetaProcessor.getMetaTags(robotsMeta, node, 
                                                  currURLsAndAnswers[i][0]);

      assertTrue("got index wrong on test " + i,
                 robotsMeta.getNoIndex() == answers[i][0]);
      assertTrue("got follow wrong on test " + i,
                 robotsMeta.getNoFollow() == answers[i][1]);
      assertTrue("got cache wrong on test " + i,
                 robotsMeta.getNoCache() == answers[i][2]);
      assertTrue("got base href wrong on test " + i + " (got "
                 + robotsMeta.getBaseHref() + ")",
                 ( (robotsMeta.getBaseHref() == null)
                    && (currURLsAndAnswers[i][1] == null) )
                 || ( (robotsMeta.getBaseHref() != null)
                      && robotsMeta.getBaseHref().equals(
                        currURLsAndAnswers[i][1]) ) );
      
    }
  }

}

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
package org.apache.nutch.analysis.lang;

import java.io.BufferedReader;
import java.io.InputStreamReader;

// Nutch imports
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.tika.language.LanguageIdentifier;
import org.junit.Assert;
import org.junit.Test;

public class TestHTMLLanguageParser {

  private static String URL = "http://foo.bar/";

  private static String BASE = "http://foo.bar/";

  String docs[] = {
      "<html lang=\"fi\"><head>document 1 title</head><body>jotain suomeksi</body></html>",
      "<html><head><meta http-equiv=\"content-language\" content=\"en\"><title>document 2 title</head><body>this is english</body></html>",
      "<html><head><meta name=\"dc.language\" content=\"en\"><title>document 3 title</head><body>this is english</body></html>" };

  // Tika does not return "fi" but null
  String metalanguages[] = { "fi", "en", "en" };

  /**
   * Test parsing of language identifiers from html
   **/
  @Test
  public void testMetaHTMLParsing() {

    try {
      ParseUtil parser = new ParseUtil(NutchConfiguration.create());
      /* loop through the test documents and validate result */
      for (int t = 0; t < docs.length; t++) {
        Content content = getContent(docs[t]);
        Parse parse = parser.parse(content).get(content.getUrl());
        Assert.assertEquals(metalanguages[t], (String) parse.getData()
            .getParseMeta().get(Metadata.LANGUAGE));
      }
    } catch (Exception e) {
      e.printStackTrace(System.out);
      Assert.fail(e.toString());
    }

  }

  /** Test of <code>LanguageParser.parseLanguage(String)</code> method. */
  @Test
  public void testParseLanguage() {
    String tests[][] = { { "(SCHEME=ISO.639-1) sv", "sv" },
        { "(SCHEME=RFC1766) sv-FI", "sv" }, { "(SCHEME=Z39.53) SWE", "sv" },
        { "EN_US, SV, EN, EN_UK", "en" }, { "English Swedish", "en" },
        { "English, swedish", "en" }, { "English,Swedish", "en" },
        { "Other (Svenska)", "sv" }, { "SE", "se" }, { "SV", "sv" },
        { "SV charset=iso-8859-1", "sv" }, { "SV-FI", "sv" },
        { "SV; charset=iso-8859-1", "sv" }, { "SVE", "sv" }, { "SW", "sw" },
        { "SWE", "sv" }, { "SWEDISH", "sv" }, { "Sv", "sv" }, { "Sve", "sv" },
        { "Svenska", "sv" }, { "Swedish", "sv" }, { "Swedish, svenska", "sv" },
        { "en, sv", "en" }, { "sv", "sv" },
        { "sv, be, dk, de, fr, no, pt, ch, fi, en", "sv" }, { "sv,en", "sv" },
        { "sv-FI", "sv" }, { "sv-SE", "sv" }, { "sv-en", "sv" },
        { "sv-fi", "sv" }, { "sv-se", "sv" },
        { "sv; Content-Language: sv", "sv" }, { "sv_SE", "sv" },
        { "sve", "sv" }, { "svenska, swedish, engelska, english", "sv" },
        { "sw", "sw" }, { "swe", "sv" }, { "swe.SPR.", "sv" },
        { "sweden", "sv" }, { "swedish", "sv" }, { "swedish,", "sv" },
        { "text/html; charset=sv-SE", "sv" }, { "text/html; sv", "sv" },
        { "torp, stuga, uthyres, bed & breakfast", null } };

    for (int i = 0; i < 44; i++) {
      Assert.assertEquals(tests[i][1],
          HTMLLanguageParser.LanguageParser.parseLanguage(tests[i][0]));
    }
  }

  private Content getContent(String text) {
    Metadata meta = new Metadata();
    meta.add("Content-Type", "text/html");
    return new Content(URL, BASE, text.getBytes(), "text/html", meta,
        NutchConfiguration.create());
  }

  @Test
  public void testLanguageIndentifier() {
    try {
      long total = 0;
      LanguageIdentifier identifier;
      BufferedReader in = new BufferedReader(new InputStreamReader(this
          .getClass().getResourceAsStream("test-referencial.txt")));
      String line = null;
      while ((line = in.readLine()) != null) {
        String[] tokens = line.split(";");
        if (!tokens[0].equals("")) {
          StringBuilder content = new StringBuilder();
          // Test each line of the file...
          BufferedReader testFile = new BufferedReader(new InputStreamReader(
              this.getClass().getResourceAsStream(tokens[0]), "UTF-8"));
          String testLine = null, lang = null;
          while ((testLine = testFile.readLine()) != null) {
            content.append(testLine + "\n");
            testLine = testLine.trim();
            if (testLine.length() > 256) {
              identifier = new LanguageIdentifier(testLine);
              lang = identifier.getLanguage();
              Assert.assertEquals(tokens[1], lang);
            }
          }
          testFile.close();

          // Test the whole file
          long start = System.currentTimeMillis();
          System.out.println(content.toString());
          identifier = new LanguageIdentifier(content.toString());
          lang = identifier.getLanguage();
          System.out.println(lang);
          total += System.currentTimeMillis() - start;
          Assert.assertEquals(tokens[1], lang);
        }
      }
      in.close();
      System.out.println("Total Time=" + total);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.toString());
    }
  }

}

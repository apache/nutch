package org.apache.nutch.parse.xsl;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseResult;
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
import org.apache.nutch.parse.xsl.XslParseFilter.PARSER;
import org.junit.Test;

/**
 * 
 * This sample test will show you how to test the crawling of a page by
 * simulating a crawl. All the thing that you have to do is to inherit from
 * AbstractCrawlTest.
 * 
 */
public class TestSample1 extends AbstractCrawlTest {

  /**
   * Loads the rules xml file that will route your transformers from urls.
   */
  public TestSample1() {
    this.getConfiguration().set(RulesManager.CONF_XML_RULES, "sample1/rules.xml");
  }

  @Test
  public void testBook1() {
    String url = "http://www.sample1.com/book?1245";

    try {
      ParseResult parseResult = simulateCrawl(PARSER.NEKO,
          new File(sampleDir, "sample1/book1.html").toString(), url);
      assertNotNull(parseResult);

      Metadata parsedMetadata = parseResult.get(url).getData().getParseMeta();
      // Asserts we have metadata
      assertNotNull(parsedMetadata);
      // Title check
      assertEquals("Nutch for dummies", parsedMetadata.get("title"));
      // Description check
      assertEquals(
          "The ultimate book to master all nutch powerful mechanisms !",
          parsedMetadata.get("description"));
      // Isbn check
      assertEquals("123654987789", parsedMetadata.get("isbn"));
      // Authors check
      assertEquals("Mr Allan A.", parsedMetadata.getValues("author")[0]);
      assertEquals("Mrs Mulan B.", parsedMetadata.getValues("author")[1]);
      // Price check
      assertEquals("free", parsedMetadata.get("price"));
      // Collection check
      assertEquals("Collection from nowhere", parsedMetadata.get("collection"));

    } catch (Exception e) {
      e.printStackTrace();
      fail("testBook1 exception");
    }
  }

}

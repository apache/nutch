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

package org.apache.nutch.indexer.replace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.basic.BasicIndexingFilter;
import org.apache.nutch.indexer.metadata.MetadataIndexer;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestIndexReplace {

  private static final String INDEX_REPLACE_PROPERTY = "index.replace.regexp";

  private String fileSeparator = System.getProperty("file.separator");
  private String sampleDir = System.getProperty("test.data", ".");
  private String sampleFile = "testIndexReplace.html";
  private String expectedDescription = "With this awesome plugin, nutch is my bitch! Bwuhuhuhaha!";
  private String expectedKeywords = "Awesome! Riveting! Two Thumbs Up!";
  private String testSimpleIndexReplaceProperty = "metatag.description=/this(.*)plugin/this awesome plugin/\n"
      + "metatag.keywords=/\\,/\\!/\n";

  /**
   * Run a test file through the Nutch parser.
   * 
   * @param fileName
   * @param conf
   * @return
   */
  public NutchDocument parseMeta(String fileName, Configuration conf) {
    NutchDocument doc = new NutchDocument();

    BasicIndexingFilter basicIndexer = new BasicIndexingFilter();
    basicIndexer.setConf(conf);
    Assert.assertNotNull(basicIndexer);

    MetadataIndexer metaIndexer = new MetadataIndexer();
    metaIndexer.setConf(conf);
    Assert.assertNotNull(basicIndexer);

    ReplaceIndexer replaceIndexer = new ReplaceIndexer();
    replaceIndexer.setConf(conf);
    Assert.assertNotNull(replaceIndexer);

    try {
      String urlString = "file:" + sampleDir + fileSeparator + fileName;
      Text text = new Text(urlString);
      CrawlDatum crawlDatum = new CrawlDatum();
      Protocol protocol = new ProtocolFactory(conf).getProtocol(urlString);
      Content content = protocol.getProtocolOutput(text, crawlDatum).getContent();
      Parse parse = new ParseUtil(conf).parse(content).get(content.getUrl());
      Metadata metadata = parse.getData().getParseMeta();
      crawlDatum.setFetchTime(100L);

      Inlinks inlinks = new Inlinks();
      doc = basicIndexer.filter(doc, parse, text, crawlDatum, inlinks);
      doc = metaIndexer.filter(doc, parse, text, crawlDatum, inlinks);
      doc = replaceIndexer.filter(doc, parse, text, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.toString());
    }

    return doc;
  }

  /**
   * Test property parsing
   */
  @Test
  public void testPropertyParse() {
    Configuration conf = NutchConfiguration.create();
    conf.set(INDEX_REPLACE_PROPERTY, testSimpleIndexReplaceProperty);

    ReplaceIndexer rp = new ReplaceIndexer();
    rp.setConf(conf);

    Configuration parsedConf = rp.getConf();

    // check that we get the same values
    Assert.assertEquals(testSimpleIndexReplaceProperty,
        parsedConf.get(INDEX_REPLACE_PROPERTY));
  }

  /**
   * Test metatag value replacement
   */
  @Test
  public void testMetatags() {
    Configuration conf = NutchConfiguration.create();
    conf.set(
        "plugin.includes",
        "protocol-file|urlfilter-regex|parse-(html|metatags)|index-(basic|anchor|metadata|static|replace)|urlnormalizer-(pass|regex|basic)");
    conf.set(INDEX_REPLACE_PROPERTY, testSimpleIndexReplaceProperty);
    conf.set("metatags.names", "author,description,keywords");
    conf.set("index.parse.md", "metatag.author,metatag.description,metatag.keywords");
    // Not necessary but helpful when debugging the filter.
    conf.set("http.timeout", "99999999999");

    // check that we get the same values
    NutchDocument doc = parseMeta(sampleFile, conf);

    Assert.assertEquals(expectedDescription, doc.getFieldValue("metatag.description"));
    Assert.assertEquals(expectedKeywords, doc.getFieldValue("metatag.keywords"));
  }
}

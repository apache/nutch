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
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * JUnit tests for the <code>index-replace</code> plugin.
 * 
 * In these tests, the sample file has some meta tags added to the Nutch
 * document by the <code>index-metadata</code> plugin. The
 * <code>index-replace</code> plugin is then used to either change (or not
 * change) the fields depending on the various values of
 * <code>index.replace.regexp</code> property being provided to Nutch.
 * 
 * 
 * @author Peter Ciuffetti
 *
 */
public class TestIndexReplace {

  private static final String INDEX_REPLACE_PROPERTY = "index.replace.regexp";

  private String fileSeparator = System.getProperty("file.separator");
  private String sampleDir = System.getProperty("test.data", ".");
  private String sampleFile = "testIndexReplace.html";

  /**
   * Run a test file through the Nutch parser and index filters.
   * 
   * @param fileName
   * @param conf
   * @return the Nutch document with the replace indexer applied
   */
  public NutchDocument parseAndFilterFile(String fileName, Configuration conf) {
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
      Content content = protocol.getProtocolOutput(text, crawlDatum)
          .getContent();
      Parse parse = new ParseUtil(conf).parse(content).get(content.getUrl());
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
   * Test property parsing.
   * 
   * The filter does not expose details of the parse. So all we are checking is
   * that the parse does not throw a runtime exception and that the value
   * provided is the value returned.
   */
  @Test
  public void testPropertyParse() {
    Configuration conf = NutchConfiguration.create();
    String indexReplaceProperty = "  metatag.description=/this(.*)plugin/this awesome plugin/2\n"
        + "  metatag.keywords=/\\,/\\!/\n"
        + "  hostmatch=.*.com\n"
        + "  metatag.keywords=/\\,/\\?/\n"
        + "  metatag.author:dc_author=/\\s+/ David /\n"
        + "  urlmatch=.*.html\n"
        + "  metatag.keywords=/\\,/\\./\n" + "  metatag.author=/\\s+/ D. /\n";

    conf.set(INDEX_REPLACE_PROPERTY, indexReplaceProperty);

    ReplaceIndexer rp = new ReplaceIndexer();
    try {
      rp.setConf(conf);
    } catch (RuntimeException ohno) {
      Assert.fail("Unable to parse a valid index.replace.regexp property! "
          + ohno.getMessage());
    }

    Configuration parsedConf = rp.getConf();

    // Does the getter equal the setter? Too easy!
    Assert.assertEquals(indexReplaceProperty,
        parsedConf.get(INDEX_REPLACE_PROPERTY));
  }

  /**
   * Test metatag value replacement using global replacement settings.
   * 
   * The index.replace.regexp property does not use hostmatch or urlmatch, so
   * all patterns are global.
   */
  @Test
  public void testGlobalReplacement() {
    String expectedDescription = "With this awesome plugin, I control the description! Bwuhuhuhaha!";
    String expectedKeywords = "Breathtaking! Riveting! Two Thumbs Up!";
    String expectedAuthor = "Peter D. Ciuffetti";
    String indexReplaceProperty = "  metatag.description=/this(.*)plugin/this awesome plugin/\n"
        + "  metatag.keywords=/\\,/\\!/\n" + "  metatag.author=/\\s+/ D. /\n";

    Configuration conf = NutchConfiguration.create();
    conf.set(
        "plugin.includes",
        "protocol-file|urlfilter-regex|parse-(html|metatags)|index-(basic|anchor|metadata|static|replace)|urlnormalizer-(pass|regex|basic)");
    conf.set(INDEX_REPLACE_PROPERTY, indexReplaceProperty);
    conf.set("metatags.names", "author,description,keywords");
    conf.set("index.parse.md",
        "metatag.author,metatag.description,metatag.keywords");
    // Not necessary but helpful when debugging the filter.
    conf.set("http.timeout", "99999999999");

    // Run the document through the parser and index filters.
    NutchDocument doc = parseAndFilterFile(sampleFile, conf);

    Assert.assertEquals(expectedDescription,
        doc.getFieldValue("metatag.description"));
    Assert
        .assertEquals(expectedKeywords, doc.getFieldValue("metatag.keywords"));
    Assert.assertEquals(expectedAuthor, doc.getFieldValue("metatag.author"));
  }

  /**
   * Test that invalid property settings are handled and ignored.
   * 
   * This test provides an invalid property setting that will fail property
   * parsing and Pattern.compile. The expected outcome is that the patterns will
   * not cause failure and the targeted fields will not be modified by the
   * filter.
   */
  @Test
  public void testInvalidPatterns() {
    String expectedDescription = "With this plugin, I control the description! Bwuhuhuhaha!";
    String expectedKeywords = "Breathtaking, Riveting, Two Thumbs Up!";
    String expectedAuthor = "Peter Ciuffetti";
    // Contains: invalid pattern, invalid flags, incomplete property
    String indexReplaceProperty = "  metatag.description=/this\\s+**plugin/this awesome plugin/\n"
        + "  metatag.keywords=/\\,/\\!/what\n" + " metatag.author=#notcomplete";

    Configuration conf = NutchConfiguration.create();
    conf.set(
        "plugin.includes",
        "protocol-file|urlfilter-regex|parse-(html|metatags)|index-(basic|anchor|metadata|static|replace)|urlnormalizer-(pass|regex|basic)");
    conf.set(INDEX_REPLACE_PROPERTY, indexReplaceProperty);
    conf.set("metatags.names", "author,description,keywords");
    conf.set("index.parse.md",
        "metatag.author,metatag.description,metatag.keywords");
    // Not necessary but helpful when debugging the filter.
    conf.set("http.timeout", "99999999999");

    // Run the document through the parser and index filters.
    NutchDocument doc = parseAndFilterFile(sampleFile, conf);

    // Assert that our metatags have not changed.
    Assert.assertEquals(expectedDescription,
        doc.getFieldValue("metatag.description"));
    Assert
        .assertEquals(expectedKeywords, doc.getFieldValue("metatag.keywords"));
    Assert.assertEquals(expectedAuthor, doc.getFieldValue("metatag.author"));

  }

  /**
   * Test URL pattern matching
   */
  @Test
  public void testUrlMatchesPattern() {
    String expectedDescription = "With this awesome plugin, I control the description! Bwuhuhuhaha!";
    String expectedKeywords = "Breathtaking! Riveting! Two Thumbs Up!";
    String expectedAuthor = "Peter D. Ciuffetti";
    String indexReplaceProperty = " urlmatch=.*.html\n"
        + "  metatag.description=/this(.*)plugin/this awesome plugin/\n"
        + "  metatag.keywords=/\\,/\\!/\n" + "  metatag.author=/\\s+/ D. /\n";

    Configuration conf = NutchConfiguration.create();
    conf.set(
        "plugin.includes",
        "protocol-file|urlfilter-regex|parse-(html|metatags)|index-(basic|anchor|metadata|static|replace)|urlnormalizer-(pass|regex|basic)");
    conf.set(INDEX_REPLACE_PROPERTY, indexReplaceProperty);
    conf.set("metatags.names", "author,description,keywords");
    conf.set("index.parse.md",
        "metatag.author,metatag.description,metatag.keywords");
    // Not necessary but helpful when debugging the filter.
    conf.set("http.timeout", "99999999999");

    // Run the document through the parser and index filters.
    NutchDocument doc = parseAndFilterFile(sampleFile, conf);

    // Assert that our metatags have changed.
    Assert.assertEquals(expectedDescription,
        doc.getFieldValue("metatag.description"));
    Assert
        .assertEquals(expectedKeywords, doc.getFieldValue("metatag.keywords"));
    Assert.assertEquals(expectedAuthor, doc.getFieldValue("metatag.author"));

  }

  /**
   * Test URL pattern not matching.
   * 
   * Expected result is that the filter does not change the fields.
   */
  @Test
  public void testUrlNotMatchesPattern() {
    String expectedDescription = "With this plugin, I control the description! Bwuhuhuhaha!";
    String expectedKeywords = "Breathtaking, Riveting, Two Thumbs Up!";
    String expectedAuthor = "Peter Ciuffetti";
    String indexReplaceProperty = " urlmatch=.*.xml\n"
        + "  metatag.description=/this(.*)plugin/this awesome plugin/\n"
        + "  metatag.keywords=/\\,/\\!/\n" + "  metatag.author=/\\s+/ D. /\n";

    Configuration conf = NutchConfiguration.create();
    conf.set(
        "plugin.includes",
        "protocol-file|urlfilter-regex|parse-(html|metatags)|index-(basic|anchor|metadata|static|replace)|urlnormalizer-(pass|regex|basic)");
    conf.set(INDEX_REPLACE_PROPERTY, indexReplaceProperty);
    conf.set("metatags.names", "author,description,keywords");
    conf.set("index.parse.md",
        "metatag.author,metatag.description,metatag.keywords");
    // Not necessary but helpful when debugging the filter.
    conf.set("http.timeout", "99999999999");

    // Run the document through the parser and index filters.
    NutchDocument doc = parseAndFilterFile(sampleFile, conf);

    // Assert that our metatags have not changed.
    Assert.assertEquals(expectedDescription,
        doc.getFieldValue("metatag.description"));
    Assert
        .assertEquals(expectedKeywords, doc.getFieldValue("metatag.keywords"));
    Assert.assertEquals(expectedAuthor, doc.getFieldValue("metatag.author"));

  }

  /**
   * Test a global pattern match for description and URL pattern match for
   * keywords and author.
   * 
   * All three should be triggered. It also tests replacement groups.
   */
  @Test
  public void testGlobalAndUrlMatchesPattern() {
    String expectedDescription = "With this awesome plugin, I control the description! Bwuhuhuhaha!";
    String expectedKeywords = "Breathtaking! Riveting! Two Thumbs Up!";
    String expectedAuthor = "Peter D. Ciuffetti";
    String indexReplaceProperty = "  metatag.description=/this(.*)plugin/this$1awesome$1plugin/\n"
        + "  urlmatch=.*.html\n"
        + "  metatag.keywords=/\\,/\\!/\n"
        + "  metatag.author=/\\s+/ D. /\n";

    Configuration conf = NutchConfiguration.create();
    conf.set(
        "plugin.includes",
        "protocol-file|urlfilter-regex|parse-(html|metatags)|index-(basic|anchor|metadata|static|replace)|urlnormalizer-(pass|regex|basic)");
    conf.set(INDEX_REPLACE_PROPERTY, indexReplaceProperty);
    conf.set("metatags.names", "author,description,keywords");
    conf.set("index.parse.md",
        "metatag.author,metatag.description,metatag.keywords");
    // Not necessary but helpful when debugging the filter.
    conf.set("http.timeout", "99999999999");

    // Run the document through the parser and index filters.
    NutchDocument doc = parseAndFilterFile(sampleFile, conf);

    // Assert that our metatags have changed.
    Assert.assertEquals(expectedDescription,
        doc.getFieldValue("metatag.description"));
    Assert
        .assertEquals(expectedKeywords, doc.getFieldValue("metatag.keywords"));
    Assert.assertEquals(expectedAuthor, doc.getFieldValue("metatag.author"));

  }

  /**
   * Test a global pattern match for description and URL pattern match for
   * keywords and author.
   * 
   * Only the global match should be triggered.
   */
  @Test
  public void testGlobalAndUrlNotMatchesPattern() {
    String expectedDescription = "With this awesome plugin, I control the description! Bwuhuhuhaha!";
    String expectedKeywords = "Breathtaking, Riveting, Two Thumbs Up!";
    String expectedAuthor = "Peter Ciuffetti";
    String indexReplaceProperty = "  metatag.description=/this(.*)plugin/this$1awesome$1plugin/\n"
        + "  urlmatch=.*.xml\n"
        + "  metatag.keywords=/\\,/\\!/\n"
        + "  metatag.author=/\\s+/ D. /\n";

    Configuration conf = NutchConfiguration.create();
    conf.set(
        "plugin.includes",
        "protocol-file|urlfilter-regex|parse-(html|metatags)|index-(basic|anchor|metadata|static|replace)|urlnormalizer-(pass|regex|basic)");
    conf.set(INDEX_REPLACE_PROPERTY, indexReplaceProperty);
    conf.set("metatags.names", "author,description,keywords");
    conf.set("index.parse.md",
        "metatag.author,metatag.description,metatag.keywords");
    // Not necessary but helpful when debugging the filter.
    conf.set("http.timeout", "99999999999");

    // Run the document through the parser and index filters.
    NutchDocument doc = parseAndFilterFile(sampleFile, conf);

    // Assert that description has changed and the others have not changed.
    Assert.assertEquals(expectedDescription,
        doc.getFieldValue("metatag.description"));
    Assert
        .assertEquals(expectedKeywords, doc.getFieldValue("metatag.keywords"));
    Assert.assertEquals(expectedAuthor, doc.getFieldValue("metatag.author"));
  }

  /**
   * Test order-specific replacement settings.
   * 
   * This makes multiple replacements on the same field and will produce the
   * expected value only if the replacements are run in the order specified.
   */
  @Test
  public void testReplacementsRunInSpecifedOrder() {
    String expectedDescription = "With this awesome plugin, I control the description! Bwuhuhuhaha!";
    String indexReplaceProperty = "  metatag.description=/this plugin/this amazing plugin/\n"
        + "  metatag.description=/this amazing plugin/this valuable plugin/\n"
        + "  metatag.description=/this valuable plugin/this cool plugin/\n"
        + "  metatag.description=/this cool plugin/this wicked plugin/\n"
        + "  metatag.description=/this wicked plugin/this awesome plugin/\n";

    Configuration conf = NutchConfiguration.create();
    conf.set(
        "plugin.includes",
        "protocol-file|urlfilter-regex|parse-(html|metatags)|index-(basic|anchor|metadata|static|replace)|urlnormalizer-(pass|regex|basic)");
    conf.set(INDEX_REPLACE_PROPERTY, indexReplaceProperty);
    conf.set("metatags.names", "author,description,keywords");
    conf.set("index.parse.md",
        "metatag.author,metatag.description,metatag.keywords");
    // Not necessary but helpful when debugging the filter.
    conf.set("http.timeout", "99999999999");

    // Run the document through the parser and index filters.
    NutchDocument doc = parseAndFilterFile(sampleFile, conf);

    // Check that the value produced by the last replacement has worked.
    Assert.assertEquals(expectedDescription,
        doc.getFieldValue("metatag.description"));
  }

  /**
   * Test a replacement pattern that uses the flags feature.
   * 
   * A 2 is Pattern.CASE_INSENSITIVE. We look for upper case and expect to match
   * any case.
   */
  @Test
  public void testReplacementsWithFlags() {
    String expectedDescription = "With this awesome plugin, I control the description! Bwuhuhuhaha!";
    String indexReplaceProperty = "  metatag.description=/THIS PLUGIN/this awesome plugin/2";

    Configuration conf = NutchConfiguration.create();
    conf.set(
        "plugin.includes",
        "protocol-file|urlfilter-regex|parse-(html|metatags)|index-(basic|anchor|metadata|static|replace)|urlnormalizer-(pass|regex|basic)");
    conf.set(INDEX_REPLACE_PROPERTY, indexReplaceProperty);
    conf.set("metatags.names", "author,description,keywords");
    conf.set("index.parse.md",
        "metatag.author,metatag.description,metatag.keywords");
    // Not necessary but helpful when debugging the filter.
    conf.set("http.timeout", "99999999999");

    // Run the document through the parser and index filters.
    NutchDocument doc = parseAndFilterFile(sampleFile, conf);

    // Check that the value produced by the case-insensitive replacement has
    // worked.
    Assert.assertEquals(expectedDescription,
        doc.getFieldValue("metatag.description"));
  }

  /**
   * Test a replacement pattern that uses the target field feature.
   * Check that the input is not modifid and that the taret field is added.
   */
  @Test
  public void testReplacementsDifferentTarget() {
    String expectedDescription = "With this plugin, I control the description! Bwuhuhuhaha!";
    String expectedTargetDescription = "With this awesome plugin, I control the description! Bwuhuhuhaha!";
    String indexReplaceProperty = "  metatag.description:new=/this plugin/this awesome plugin/";

    Configuration conf = NutchConfiguration.create();
    conf.set(
        "plugin.includes",
        "protocol-file|urlfilter-regex|parse-(html|metatags)|index-(basic|anchor|metadata|static|replace)|urlnormalizer-(pass|regex|basic)");
    conf.set(INDEX_REPLACE_PROPERTY, indexReplaceProperty);
    conf.set("metatags.names", "author,description,keywords");
    conf.set("index.parse.md",
        "metatag.author,metatag.description,metatag.keywords");
    // Not necessary but helpful when debugging the filter.
    conf.set("http.timeout", "99999999999");

    // Run the document through the parser and index filters.
    NutchDocument doc = parseAndFilterFile(sampleFile, conf);

    // Check that the input field has not been modified
    Assert.assertEquals(expectedDescription,
        doc.getFieldValue("metatag.description"));
    // Check that the output field has created
    Assert.assertEquals(expectedTargetDescription,
        doc.getFieldValue("new"));
  }
}

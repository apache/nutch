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

package org.apache.nutch.indexer;

import java.lang.invoke.MethodHandles;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

/** Test {@link IndexerMapReduce} */
public class TestIndexerMapReduce {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static String testUrl = "http://nutch.apache.org/";
  public static Text testUrlText = new Text(testUrl);
  public static String htmlContentType = "text/html";
  public static String testHtmlDoc = "<!DOCTYPE html>\n"
      + "<html>\n"
      + "<head>\n"
      + "<title>Test Indexing Binary Content</title>\n"
      + "<meta charset=\"utf-8\">\n"
      + "<meta name=\"keywords\" lang=\"en\" content=\"charset, encoding\" />\n"
      + "<meta name=\"keywords\" lang=\"fr\" content=\"codage des caractères\" />\n"
      + "<meta name=\"keywords\" lang=\"cs\" content=\"kódování znaků\" />\n"
      + "</head>\n"
      + "<body>\n"
      + "<p>\n"
      + "<ul>\n"
      + "  <li lang=\"en\">English: character set, encoding\n"
      + "  <li lang=\"fr\">Français: codage des caractères\n"
      + "  <li lang=\"cs\">Čeština: kódování znaků (not covered by Latin-1)\n"
      + "</ul>\n"
      + "</body>\n"
      + "</html>";
  public static Metadata htmlMeta = new Metadata();
  static {
    htmlMeta.add("Content-Type", "text/html");
    // add segment and signature to avoid NPEs
    htmlMeta.add(Nutch.SEGMENT_NAME_KEY, "123");
    htmlMeta.add(Nutch.SIGNATURE_KEY, "123");
  }
  public static ParseText parseText = new ParseText("Test");
  public static ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS,
      "Test", new Outlink[] {}, htmlMeta);
  public static CrawlDatum crawlDatumDbFetched = new CrawlDatum(
      CrawlDatum.STATUS_DB_FETCHED, 60 * 60 * 24);
  public static CrawlDatum crawlDatumFetchSuccess = new CrawlDatum(
      CrawlDatum.STATUS_FETCH_SUCCESS, 60 * 60 * 24);

  private Reducer<Text, NutchWritable, Text, NutchIndexAction> reducer = new IndexerMapReduce.IndexerReducer();
  private ReduceDriver<Text, NutchWritable, Text, NutchIndexAction> reduceDriver;
  private Configuration configuration;


  /**
   * Test indexing of base64-encoded binary content.
   */
  @Test
  public void testBinaryContentBase64() {
    configuration = NutchConfiguration.create();
    configuration.setBoolean(IndexerMapReduce.INDEXER_BINARY_AS_BASE64, true);

    Charset[] testCharsets = { StandardCharsets.UTF_8,
        Charset.forName("iso-8859-1"), Charset.forName("iso-8859-2") };
    for (Charset charset : testCharsets) {
      LOG.info("Testing indexing binary content as base64 for charset {}",
          charset.name());

      String htmlDoc = testHtmlDoc;
      if (charset != StandardCharsets.UTF_8) {
        htmlDoc = htmlDoc.replaceAll("utf-8", charset.name());
        if (charset.name().equalsIgnoreCase("iso-8859-1")) {
          // Western-European character set: remove Czech content
          htmlDoc = htmlDoc.replaceAll("\\s*<[^>]+\\slang=\"cs\".+?\\n", "");
        } else if (charset.name().equalsIgnoreCase("iso-8859-2")) {
          // Eastern-European character set: remove French content
          htmlDoc = htmlDoc.replaceAll("\\s*<[^>]+\\slang=\"fr\".+?\\n", "");
        }
      }

      Content content = new Content(testUrl, testUrl,
          htmlDoc.getBytes(charset), htmlContentType, htmlMeta,
          configuration);

      NutchDocument doc = runIndexer(crawlDatumDbFetched,
          crawlDatumFetchSuccess, parseText, parseData, content);
      assertNotNull("No NutchDocument indexed", doc);

      String binaryContentBase64 = (String) doc.getField("binaryContent")
          .getValues().get(0);
      LOG.info("binary content (base64): {}", binaryContentBase64);
      String binaryContent = new String(
          Base64.decodeBase64(binaryContentBase64), charset);
      LOG.info("binary content (decoded): {}", binaryContent);
      assertEquals(
          "Binary content (" + charset + ") not correctly saved as base64",
          htmlDoc, binaryContent);
    }
  }

  /**
   * Run {@link IndexerMapReduce.reduce(...)} to get a &quot;indexed&quot;
   * {@link NutchDocument} by passing objects from segment and CrawlDb to the
   * indexer.
   *
   * @param dbDatum
   *          crawl datum from CrawlDb
   * @param fetchDatum
   *          crawl datum (fetch status) from segment
   * @param parseText
   *          plain text from parsed document
   * @param parseData
   *          parse data
   * @param content
   *          (optional, if index binary content) protocol content
   * @return &quot;indexed&quot; document
   */
  public NutchDocument runIndexer(CrawlDatum dbDatum, CrawlDatum fetchDatum,
      ParseText parseText, ParseData parseData, Content content) {
    List<NutchWritable> values = new ArrayList<NutchWritable>();
    values.add(new NutchWritable(dbDatum));
    values.add(new NutchWritable(fetchDatum));
    values.add(new NutchWritable(parseText));
    values.add(new NutchWritable(parseData));
    values.add(new NutchWritable(content));
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    reduceDriver.getConfiguration().addResource(configuration);
    reduceDriver.withInput(testUrlText, values);
    List<Pair<Text, NutchIndexAction>> reduceResult;
    NutchDocument doc = null;
    try {
      reduceResult = reduceDriver.run();
      for (Pair<Text, NutchIndexAction> p : reduceResult) {
        if (p.getSecond().action != NutchIndexAction.DELETE) {
          doc = p.getSecond().doc;
        }
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    return doc;
  }

}

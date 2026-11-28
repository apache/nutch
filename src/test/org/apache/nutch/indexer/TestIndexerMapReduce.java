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
package org.apache.nutch.indexer;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.nutch.metrics.NutchMetrics;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.ReducerContextWrapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tdunning.math.stats.MergingDigest;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.nutch.metrics.LatencyTestUtil.assertCountAndSum;
import static org.apache.nutch.metrics.LatencyTestUtil.assertCountSumAndPercentilesZero;
import static org.apache.nutch.metrics.LatencyTestUtil.assertPercentilesZero;

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
  public static ParseText failedParseText = new ParseText("Failed Parse");
  public static ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS,
      "Test", new Outlink[] {}, htmlMeta);
  public static ParseData failedParseData = new ParseData(ParseStatus.STATUS_FAILURE,
      "Failed Parse", new Outlink[] {}, htmlMeta);
  public static CrawlDatum crawlDatumDbFetched = new CrawlDatum(
      CrawlDatum.STATUS_DB_FETCHED, 60 * 60 * 24);
  public static CrawlDatum crawlDatumFetchSuccess = new CrawlDatum(
      CrawlDatum.STATUS_FETCH_SUCCESS, 60 * 60 * 24);
  public static CrawlDatum crawlDatumDbParseFailed = new CrawlDatum(
      CrawlDatum.STATUS_DB_PARSE_FAILED, 60 * 60 * 24);
  public static CrawlDatum crawlDatumParseFailed = new CrawlDatum(
      CrawlDatum.STATUS_PARSE_FAILED, 60 * 60 * 24);

  private IndexerMapReduce.IndexerReducer reducer = new IndexerMapReduce.IndexerReducer();

  private Configuration configuration;

  @Test
  public void testDeleteParseFailure() {
    configuration = NutchConfiguration.create();
    configuration.setBoolean(ParseSegment.DELETE_FAILED_PARSE, true);

    // unrelated issue with "index.jexl.filter", don't use all plugins.  Ref: src/test/nutch-site.xml
    configuration.set("plugin.includes", "protocol-http|urlfilter-regex|parse-(html|tika)|index-(basic|anchor)|indexer-csv|scoring-opic|urlnormalizer-(pass|regex|basic)");

    int docToDelete = 0;
    int docsToIndex = 0;
    int deletedDocs = 0;
    int indexedDocs = 0;
    for(int i=0; i<5; i++) {
      boolean failParse = i % 2 == 0;
      ParseData data = null;
      ParseText text = null;
      CrawlDatum dbStatus = null;
      CrawlDatum status = null;

      Content content = new Content(testUrl, testUrl,
          testHtmlDoc.getBytes(StandardCharsets.UTF_8), htmlContentType, htmlMeta,
          configuration);

      if(failParse) {
        data = failedParseData;
        text = failedParseText;
        dbStatus = crawlDatumDbParseFailed;
        status = crawlDatumParseFailed;
        docToDelete++;
      } else {
        data = parseData;
        text = parseText;
        dbStatus = crawlDatumDbFetched;
        status = crawlDatumFetchSuccess;
        docsToIndex++;
      }

      NutchDocument doc = runIndexer(dbStatus, status, text, data, content);
      if(failParse) {
        deletedDocs++;
        assertNull(doc, "NutchDocument should not be indexed");
      } else {
        indexedDocs++;
        assertNotNull(doc, "No NutchDocument indexed");
      }
      assertEquals(docToDelete, deletedDocs);
      assertEquals(docsToIndex, indexedDocs);
    }
  }

  /**
   * Test indexing of base64-encoded binary content.
   */
  @Test
  public void testBinaryContentBase64() {
    configuration = NutchConfiguration.create();
    configuration.setBoolean(IndexerMapReduce.INDEXER_BINARY_AS_BASE64, true);

    // unrelated issue with "index.jexl.filter", don't use all plugins.  Ref: src/test/nutch-site.xml
    configuration.set("plugin.includes", "protocol-http|urlfilter-regex|parse-(html|tika)|index-(basic|anchor)|indexer-csv|scoring-opic|urlnormalizer-(pass|regex|basic)");

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
      assertNotNull(doc, "No NutchDocument indexed");

      String binaryContentBase64 = (String) doc.getField("binaryContent")
          .getValues().get(0);
      LOG.info("binary content (base64): {}", binaryContentBase64);
      String binaryContent = new String(
          Base64.decodeBase64(binaryContentBase64), charset);
      LOG.info("binary content (decoded): {}", binaryContent);
      assertEquals(htmlDoc, binaryContent,
          "Binary content (" + charset + ") not correctly saved as base64");
    }
  }

  /**
   * Two reduce tasks with disjoint latency samples. Hadoop sums each counter
   * across tasks, so the indexer must not publish per-task percentiles as
   * counters: summed medians are not the median of the union.
   */
  @Test
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testLatencyPercentilesNotSummedAcrossReducers() throws Exception {
    configuration = newLatencyConfiguration();
    Counters jobCounters = new Counters();
    // Task 0: 10..50 ms, task 1: 500..900 ms. Union count 10, sum 3650.
    jobCounters.incrAllCounters(runLatencyReducer(new long[] { 10, 20, 30, 40, 50 }, null));
    jobCounters.incrAllCounters(runLatencyReducer(new long[] { 500, 600, 700, 800, 900 }, null));

    assertCountAndSum(jobCounters, NutchMetrics.GROUP_INDEXER,
        NutchMetrics.INDEXER_LATENCY, 10, 3650);
    assertPercentilesZero(jobCounters, NutchMetrics.GROUP_INDEXER,
        NutchMetrics.INDEXER_LATENCY);
  }

  /**
   * Two reduce tasks write their digests under the job output. The driver merges
   * them, so the percentiles describe the union of all samples.
   */
  @Test
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testMergeLatencyDigestsAcrossReducers(@TempDir File tempDir) throws Exception {
    configuration = newLatencyConfiguration();
    configuration.set(FileOutputFormat.OUTDIR, tempDir.toURI().toString());

    runLatencyReducer(new long[] { 10, 20, 30, 40, 50 }, attemptId(0));
    runLatencyReducer(new long[] { 500, 600, 700, 800, 900 }, attemptId(1));

    File latencyDir = new File(tempDir, IndexerMapReduce.LATENCY_DIR);
    String[] digestFiles = latencyDir.list(
        (dir, name) -> name.endsWith(IndexerMapReduce.LATENCY_DIGEST_SUFFIX));
    assertEquals(2, digestFiles == null ? 0 : digestFiles.length,
        "expected one digest file per reduce task");

    MergingDigest merged = IndexerMapReduce.mergeLatencyDigests(configuration,
        new Path(tempDir.toURI()));
    assertNotNull(merged, "merged digest expected");
    assertEquals(10, merged.size());
    // Median of the union lies between the two groups. A sum of per-task
    // medians (about 30 + 700) would fall outside this range.
    double p50 = merged.quantile(0.50);
    double p95 = merged.quantile(0.95);
    double p99 = merged.quantile(0.99);
    assertTrue(p50 >= 50 && p50 <= 500, "p50=" + p50 + " not in [50,500]");
    assertTrue(p95 >= 500 && p95 <= 900, "p95=" + p95 + " not in [500,900]");
    assertTrue(p99 >= 500 && p99 <= 900, "p99=" + p99 + " not in [500,900]");
  }

  /**
   * A reduce task with no samples writes no digest and publishes zero count,
   * sum and percentiles. Merging then finds nothing, for a missing or empty output.
   */
  @Test
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testNoLatencyDigestWithoutSamples(@TempDir File tempDir) throws Exception {
    configuration = newLatencyConfiguration();
    configuration.set(FileOutputFormat.OUTDIR, tempDir.toURI().toString());

    Counters counters = runLatencyReducer(new long[0], attemptId(0));

    assertCountSumAndPercentilesZero(counters, NutchMetrics.GROUP_INDEXER,
        NutchMetrics.INDEXER_LATENCY);
    assertFalse(new File(tempDir, IndexerMapReduce.LATENCY_DIR).exists(),
        "no digest directory expected for empty input");
    assertNull(IndexerMapReduce.mergeLatencyDigests(configuration,
        new Path(tempDir.toURI())));
    assertNull(IndexerMapReduce.mergeLatencyDigests(configuration,
        new Path(new File(tempDir, "missing").toURI())));
  }

  private static TaskAttemptID attemptId(int partition) {
    TaskID taskId = new TaskID(new JobID("test", 1), TaskType.REDUCE, partition);
    return new TaskAttemptID(taskId, 0);
  }

  /**
   * Configuration for latency tests: same plugin selection as the other tests in
   * this class, so that {@link IndexerMapReduce.IndexerReducer#setup} succeeds.
   */
  private Configuration newLatencyConfiguration() {
    Configuration conf = NutchConfiguration.create();
    conf.set("plugin.includes", "protocol-http|urlfilter-regex|parse-(html|tika)|index-(basic|anchor)|indexer-csv|scoring-opic|urlnormalizer-(pass|regex|basic)");
    return conf;
  }

  /**
   * Runs one reducer instance through setup, records the given samples, and runs
   * cleanup, as a single reduce task would.
   *
   * @return the counters that task published
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private Counters runLatencyReducer(long[] samples, TaskAttemptID attemptId)
      throws Exception {
    IndexerMapReduce.IndexerReducer latencyReducer = new IndexerMapReduce.IndexerReducer();
    ReducerContextWrapper contextWrapper = new ReducerContextWrapper(latencyReducer,
        configuration, new HashMap<>(), attemptId);
    latencyReducer.setup(contextWrapper.getContext());
    for (long sample : samples) {
      latencyReducer.getLatencyTracker().record(sample);
    }
    latencyReducer.cleanup(contextWrapper.getContext());
    return contextWrapper.getCounters();
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
   * @throws InterruptedException
   * @throws IOException
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public NutchDocument runIndexer(CrawlDatum dbDatum, CrawlDatum fetchDatum,
      ParseText parseText, ParseData parseData, Content content) {
    List<NutchWritable> values = new ArrayList<NutchWritable>();
    values.add(new NutchWritable(dbDatum));
    values.add(new NutchWritable(fetchDatum));
    values.add(new NutchWritable(parseText));
    values.add(new NutchWritable(parseData));
    values.add(new NutchWritable(content));
    Map<Text, NutchIndexAction> reduceResult = new HashMap<>();
    ReducerContextWrapper contextWrapper = new ReducerContextWrapper(reducer, configuration, reduceResult);
    NutchDocument doc = null;
    try {
      reducer.setup(contextWrapper.getContext());
      // test
      reducer.reduce(testUrlText, values, contextWrapper.getContext());

      for (Map.Entry<Text, NutchIndexAction> e : reduceResult.entrySet()) {
        if (e.getValue().action != NutchIndexAction.DELETE) {
          doc = e.getValue().doc;
        }
      }
    } catch (IOException | InterruptedException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    return doc;
  }

}

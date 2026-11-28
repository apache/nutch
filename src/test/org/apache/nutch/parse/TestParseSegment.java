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
package org.apache.nutch.parse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.metrics.LatencyTestUtil;
import org.apache.nutch.metrics.NutchMetrics;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.ReducerContextWrapper;
import org.junit.jupiter.api.Test;

public class TestParseSegment {
  private static byte[] BYTES = "the quick brown fox".getBytes(StandardCharsets.UTF_8);

  @Test
  public void testMetadataFlag() throws Exception {

    Content content = new Content();
    Metadata metadata = new Metadata();
    metadata.set(Response.TRUNCATED_CONTENT, "true");
    content.setMetadata(metadata);
    content.setContent(BYTES);
    assertTrue(ParseSegment.isTruncated(content));

    metadata.set(Response.TRUNCATED_CONTENT, "false");
    assertFalse(ParseSegment.isTruncated(content));

    //test that truncated_content does override length field
    metadata = new Metadata();
    metadata.set(Response.TRUNCATED_CONTENT, "false");
    metadata.set(Response.CONTENT_LENGTH, Integer.toString(BYTES.length + 10));
    assertFalse(ParseSegment.isTruncated(content));

    //test that truncated_content does override length field
    metadata = new Metadata();
    metadata.set(Response.TRUNCATED_CONTENT, "true");
    metadata.set(Response.CONTENT_LENGTH, Integer.toString(BYTES.length));
    assertFalse(ParseSegment.isTruncated(content));

  }

  @Test
  public void testLength() throws Exception {
    Content content = new Content();
    Metadata metadata = new Metadata();
    metadata.set(Response.CONTENT_LENGTH, Integer.toString(BYTES.length));
    content.setMetadata(metadata);
    content.setContent(BYTES);
    assertFalse(ParseSegment.isTruncated(content));

    metadata.set(Response.CONTENT_LENGTH, Integer.toString(BYTES.length * 2));
    assertTrue(ParseSegment.isTruncated(content));
  }

  @Test
  public void testNoLengthField() {
    //test return false if there is no "Length" header field
    Content content = new Content();
    Metadata metadata = new Metadata();
    content.setMetadata(metadata);
    content.setContent(BYTES);
    assertFalse(ParseSegment.isTruncated(content));
  }

  /**
   * ParseSegmentReducer latency branch: when key is LATENCY_KEY, merges
   * BytesWritable TDigests and sets job-level parser latency counters.
   */
  @Test
  void testParseSegmentReducerLatencyKeySetsCounters() throws IOException, InterruptedException {
    Configuration conf = NutchConfiguration.create();
    Map<Text, ParseImpl> out = new HashMap<>();
    ParseSegment.ParseSegmentReducer reducer = new ParseSegment.ParseSegmentReducer();
    ReducerContextWrapper<Text, NutchWritable, Text, ParseImpl> wrapper = new ReducerContextWrapper<>(
        reducer, conf, out);

    byte[] digestBytes = LatencyTestUtil.createDigestBytes(100, 200);
    List<NutchWritable> values = new ArrayList<>();
    values.add(new NutchWritable(new BytesWritable(digestBytes)));

    reducer.reduce(new Text(NutchMetrics.LATENCY_KEY), values,
        wrapper.getContext());

    LatencyTestUtil.assertPercentilesInRange(wrapper.getCounters(),
        NutchMetrics.GROUP_PARSER, NutchMetrics.PARSER_LATENCY, 100, 200);
    assertEquals(0, out.size());
  }

  /**
   * ParseSegmentPartitioner sends LATENCY_KEY to partition 0 so one reducer
   * merges all TDigests.
   */
  @Test
  void testParseSegmentPartitionerSendsLatencyKeyToPartitionZero() {
    ParseSegment.ParseSegmentPartitioner partitioner = new ParseSegment.ParseSegmentPartitioner();
    int numPartitions = 4;
    assertEquals(0, partitioner.getPartition(new Text(NutchMetrics.LATENCY_KEY),
        new NutchWritable(new BytesWritable()), numPartitions));
  }

  @Test
  void testParseSegmentPartitionerWithSinglePartition() {
    ParseSegment.ParseSegmentPartitioner partitioner = new ParseSegment.ParseSegmentPartitioner();
    assertEquals(0, partitioner.getPartition(new Text(NutchMetrics.LATENCY_KEY),
        new NutchWritable(new BytesWritable()), 1));
    assertEquals(0, partitioner.getPartition(new Text("http://example.com/"),
        new NutchWritable(new BytesWritable()), 1));
  }

  private static Content contentWithFetchStatus(String... values) {
    Content content = new Content();
    Metadata metadata = new Metadata();
    for (String value : values) {
      metadata.add(Nutch.FETCH_STATUS_KEY, value);
    }
    content.setMetadata(metadata);
    content.setContent(BYTES);
    return content;
  }

  @Test
  public void testFetchStatus() {
    assertEquals(CrawlDatum.STATUS_FETCH_SUCCESS,
        ParseSegment.getFetchStatus(contentWithFetchStatus(
            Integer.toString(CrawlDatum.STATUS_FETCH_SUCCESS))));
    assertEquals(CrawlDatum.STATUS_PARSE_FAILED,
        ParseSegment.getFetchStatus(contentWithFetchStatus(
            Integer.toString(CrawlDatum.STATUS_PARSE_FAILED))));
    // no fetch status
    assertEquals(ParseSegment.MISSING_FETCH_STATUS,
        ParseSegment.getFetchStatus(contentWithFetchStatus()));
  }

  /**
   * NUTCH-3252: a malformed fetch status (e.g. a server sent a "_fst_"
   * response header) must skip the record, not fail the parse job.
   */
  @Test
  public void testMalformedFetchStatus() {
    for (String bad : new String[] { "not-a-number", "", " ", "33.0",
        "99999999999" }) {
      assertEquals(ParseSegment.MISSING_FETCH_STATUS,
          ParseSegment.getFetchStatus(contentWithFetchStatus(bad)),
          "malformed fetch status: '" + bad + "'");
    }
    // segments written before the fix hold the header value first
    assertEquals(ParseSegment.MISSING_FETCH_STATUS,
        ParseSegment.getFetchStatus(contentWithFetchStatus("not-a-number",
            Integer.toString(CrawlDatum.STATUS_FETCH_SUCCESS))));
  }

  /**
   * NUTCH-3252: records with a missing or malformed fetch status are skipped
   * and counted; an unsuccessful fetch status is skipped but not counted.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testMapperCountsInvalidFetchStatus() throws Exception {
    Map<String, Counter> counters = new HashMap<>();
    Mapper<WritableComparable<?>, Content, Text, NutchWritable>.Context
        context = mock(Mapper.Context.class);
    when(context.getConfiguration()).thenReturn(NutchConfiguration.create());
    when(context.getCounter(anyString(), anyString())).thenAnswer(
        inv -> counters.computeIfAbsent(
            inv.getArgument(0) + "/" + inv.getArgument(1),
            k -> new GenericCounter()));

    ParseSegment.ParseSegmentMapper mapper =
        new ParseSegment.ParseSegmentMapper();
    mapper.setup(context);
    Text key = new Text("http://example.com/");
    mapper.map(key, contentWithFetchStatus(), context);
    mapper.map(key, contentWithFetchStatus("not-a-number"), context);
    mapper.map(key, contentWithFetchStatus(
        Integer.toString(CrawlDatum.STATUS_FETCH_GONE)), context);

    assertEquals(2, counters.get(NutchMetrics.GROUP_PARSER + "/"
        + NutchMetrics.PARSER_INVALID_FETCH_STATUS_TOTAL).getValue());
    verify(context, never()).write(any(), any());
  }
}

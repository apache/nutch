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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.metadata.Metadata;
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
    ReducerContextWrapper<Text, Writable, Text, ParseImpl> wrapper =
        new ReducerContextWrapper<>(reducer, conf, out);

    byte[] digestBytes = LatencyTestUtil.createDigestBytes(100, 200);
    List<Writable> values = new ArrayList<>();
    values.add(new BytesWritable(digestBytes));

    reducer.reduce(new Text(NutchMetrics.LATENCY_KEY), values, wrapper.getContext());

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
    assertEquals(0, partitioner.getPartition(new Text(NutchMetrics.LATENCY_KEY), new BytesWritable(), numPartitions));
  }

  @Test
  void testParseSegmentPartitionerWithSinglePartition() {
    ParseSegment.ParseSegmentPartitioner partitioner = new ParseSegment.ParseSegmentPartitioner();
    assertEquals(0, partitioner.getPartition(new Text(NutchMetrics.LATENCY_KEY), new BytesWritable(), 1));
    assertEquals(0, partitioner.getPartition(new Text("http://example.com/"), new BytesWritable(), 1));
  }
}

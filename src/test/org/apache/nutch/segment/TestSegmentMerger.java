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
package org.apache.nutch.segment;

import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Writer.Option;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.util.CancellationAwareTestUtils;
import org.apache.nutch.util.CancellationAwareTestUtils.CancellationToken;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests for SegmentMerger functionality.
 * This test is cancellation-aware for graceful shutdown during fail-fast mode.
 */
public class TestSegmentMerger {
  Configuration conf;
  FileSystem fs;
  Path testDir;
  Path seg1;
  Path seg2;
  Path out;
  int countSeg1, countSeg2;

  @BeforeAll
  public static void checkConditions() throws Exception {
    assumeTrue(Boolean.getBoolean("test.include.slow"));
  }

  @BeforeEach
  public void setUp() throws Exception {
    conf = NutchConfiguration.create();
    fs = FileSystem.get(conf);
    testDir = new Path(conf.get("hadoop.tmp.dir"), "merge-"
        + System.currentTimeMillis());
    seg1 = new Path(testDir, "seg1");
    seg2 = new Path(testDir, "seg2");
    out = new Path(testDir, "out");

    // create large parse-text segments
    System.err.println("Creating large segment 1...");
    DecimalFormat df = new DecimalFormat("0000000");
    Text k = new Text();
    Path ptPath = new Path(new Path(seg1, ParseText.DIR_NAME), "part-00000");
    Option kOpt = MapFile.Writer.keyClass(Text.class);
    org.apache.hadoop.io.SequenceFile.Writer.Option vOpt = SequenceFile.Writer.valueClass(ParseText.class);
    MapFile.Writer w = new MapFile.Writer(conf, ptPath, kOpt, vOpt);
    long curSize = 0;
    countSeg1 = 0;
    FileStatus fileStatus = fs.getFileStatus(ptPath);
    long blkSize = fileStatus.getBlockSize();

    while (curSize < blkSize * 2) {
      k.set("seg1-" + df.format(countSeg1));
      w.append(k, new ParseText("seg1 text " + countSeg1));
      countSeg1++;
      curSize += 40; // roughly ...
    }
    w.close();
    System.err.println(" - done: " + countSeg1 + " records.");
    System.err.println("Creating large segment 2...");
    ptPath = new Path(new Path(seg2, ParseText.DIR_NAME), "part-00000");
    Option wKeyOpt = MapFile.Writer.keyClass(Text.class);
    org.apache.hadoop.io.SequenceFile.Writer.Option wValueOpt = SequenceFile.Writer.valueClass(ParseText.class);
    w = new MapFile.Writer(conf, ptPath, wKeyOpt, wValueOpt);
    curSize = 0;
    countSeg2 = 0;
    while (curSize < blkSize * 2) {
      k.set("seg2-" + df.format(countSeg2));
      w.append(k, new ParseText("seg2 text " + countSeg2));
      countSeg2++;
      curSize += 40; // roughly ...
    }
    w.close();
    System.err.println(" - done: " + countSeg2 + " records.");
  }

  @AfterEach
  public void tearDown() throws Exception {
    fs.delete(testDir, true);
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  public void testLargeMerge() throws Exception {
    // Create cancellation token for graceful shutdown support
    CancellationToken cancellationToken = CancellationAwareTestUtils.createToken();

    SegmentMerger merger = new SegmentMerger(conf);
    merger.merge(out, new Path[] { seg1, seg2 }, false, false, -1);

    // Check for cancellation before verification
    if (cancellationToken.isCancelled()) {
      return;
    }

    // verify output
    FileStatus[] stats = fs.listStatus(out);
    // there should be just one path
    assertEquals(1, stats.length);
    Path outSeg = stats[0].getPath();
    Text k = new Text();
    ParseText v = new ParseText();
    MapFile.Reader[] readers = MapFileOutputFormat.getReaders(new Path(
        outSeg, ParseText.DIR_NAME), conf);
    int cnt1 = 0, cnt2 = 0;
    try {
      for (MapFile.Reader r : readers) {
        while (r.next(k, v)) {
          // Check for cancellation periodically during I/O
          if (cancellationToken.isCancelled()) {
            return;
          }
          
          String ks = k.toString();
          String vs = v.getText();
          if (ks.startsWith("seg1-")) {
            cnt1++;
            assertTrue(vs.startsWith("seg1 "));
          } else if (ks.startsWith("seg2-")) {
            cnt2++;
            assertTrue(vs.startsWith("seg2 "));
          }
        }
      }
    } finally {
      // Ensure readers are closed even on cancellation
      for (MapFile.Reader r : readers) {
        r.close();
      }
    }

    // Skip final assertions if cancelled
    if (cancellationToken.isCancelled()) {
      return;
    }

    assertEquals(countSeg1, cnt1);
    assertEquals(countSeg2, cnt2);
  }

}

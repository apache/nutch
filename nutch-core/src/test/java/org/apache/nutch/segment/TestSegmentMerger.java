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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Writer.Option;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSegmentMerger {
  Configuration conf;
  FileSystem fs;
  Path testDir;
  Path seg1;
  Path seg2;
  Path out;
  int countSeg1, countSeg2;

  @Before
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

  @After
  public void tearDown() throws Exception {
    fs.delete(testDir, true);
  }

  @Test
  public void testLargeMerge() throws Exception {
    SegmentMerger merger = new SegmentMerger(conf);
    merger.merge(out, new Path[] { seg1, seg2 }, false, false, -1);
    // verify output
    FileStatus[] stats = fs.listStatus(out);
    // there should be just one path
    Assert.assertEquals(1, stats.length);
    Path outSeg = stats[0].getPath();
    Text k = new Text();
    ParseText v = new ParseText();
    MapFile.Reader[] readers = MapFileOutputFormat.getReaders(fs, new Path(
        outSeg, ParseText.DIR_NAME), conf);
    int cnt1 = 0, cnt2 = 0;
    for (MapFile.Reader r : readers) {
      while (r.next(k, v)) {
        String ks = k.toString();
        String vs = v.getText();
        if (ks.startsWith("seg1-")) {
          cnt1++;
          Assert.assertTrue(vs.startsWith("seg1 "));
        } else if (ks.startsWith("seg2-")) {
          cnt2++;
          Assert.assertTrue(vs.startsWith("seg2 "));
        }
      }
      r.close();
    }
    Assert.assertEquals(countSeg1, cnt1);
    Assert.assertEquals(countSeg2, cnt2);
  }

}

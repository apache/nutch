/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.tools;

import java.io.*;
import java.util.Properties;
import java.util.Random;

import org.apache.nutch.db.Page;
import org.apache.nutch.fetcher.FetcherOutput;
import org.apache.nutch.io.ArrayFile;
import org.apache.nutch.io.MD5Hash;
import org.apache.nutch.fs.*;
import org.apache.nutch.segment.SegmentReader;
import org.apache.nutch.segment.SegmentWriter;
import org.apache.nutch.util.*;
import org.apache.nutch.pagedb.FetchListEntry;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;

import junit.framework.TestCase;

/** Unit tests for SegmentMergeTool methods. */
public class TestSegmentMergeTool extends TestCase {

  protected static final int SEGMENT_CNT = 10;

  protected static final int PAGE_CNT = 500;

  protected File testDir = null;

  public TestSegmentMergeTool(String name) {
    super(name);
  }

  /**
   * Create test directory.
   * 
   * @see junit.framework.TestCase#setUp()
   */
  protected void setUp() throws Exception {
    super.setUp();
    testDir = File.createTempFile(".smttest", "");
    testDir.delete();
    testDir.mkdirs();
  }

  /**
   * Create test segment data.
   * 
   * @param dir segment directory
   * @param unique if true, use unique data per segment, otherwise use the same
   *        data
   * @throws Exception
   */
  protected void createSegmentData(NutchFileSystem nfs, File dir, boolean unique) throws Exception {
    SegmentWriter sw = new SegmentWriter(nfs, dir, true);
    Random r = new Random(System.currentTimeMillis());
    for (int i = 0; i < PAGE_CNT; i++) {
      String url = "http://www.example.com/page-" + i;
      String rnd = "";
      if (unique) {
        rnd = "/" + System.currentTimeMillis();
        url += rnd;
      }
      url += "/example.html";
      FetchListEntry fle = new FetchListEntry(true, new Page(url, 1.0f), new String[] { "test" + rnd });
      FetcherOutput fo = new FetcherOutput(fle, MD5Hash.digest(url), FetcherOutput.SUCCESS);
      StringBuffer content = new StringBuffer("<html><body><h1>Hello from Page " + i + "</h1>");
      if (unique) {
        content.append("<p>Created at epoch time: " + System.currentTimeMillis() + ", " + r.nextLong() + "</p>");
      }
      for (int k = 0; k < 10; k++) {
        content.append("<p>" + k + " lines of text in the queue, " + k + " lines of text...</p>\n");
      }
      content.append("</body></html>");
      Properties meta = new Properties();
      meta.setProperty("Content-Type", "text/html");
      meta.setProperty("Host", "http://localhost");
      meta.setProperty("Connection", "Keep-alive, close");
      Content co = new Content(url, "http://www.example.com", content.toString().getBytes("UTF-8"), "text/html", meta);
      ParseData pd = new ParseData("Hello from Page " + i, new Outlink[0], meta);
      StringBuffer text = new StringBuffer("Hello from Page" + i);
      if (unique) {
        text.append("\nCreated at epoch time: " + System.currentTimeMillis() + ", " + r.nextLong());
      }
      for (int k = 0; k < 10; k++) {
        text.append(k + " lines of text in the queue, " + k + " lines of text...\n");
      }
      ParseText pt = new ParseText(text.toString());
      sw.append(fo, co, pt, pd);
    }
    sw.close();
  }

  /**
   * Remove test directory.
   * 
   * @see junit.framework.TestCase#tearDown()
   */
  protected void tearDown() throws Exception {
    NutchFileSystem nfs = new LocalFileSystem();
    try {
      super.tearDown();
      try {
        FileUtil.fullyDelete(nfs, testDir);
      } catch (Exception e) {
        System.out.println("NON-FATAL: " + e.getMessage());
      }
    } finally {
      nfs.close();
    }
  }

  /**
   * Test merging segments with unique data. The output (merged segment) should
   * contain the number of pages equal exactly to a product of segment count
   * times page count per segment.
   *  
   */
  public void testUniqueMerge() throws IOException {
    NutchFileSystem nfs = new LocalFileSystem();
    try {
      File dataDir = new File(testDir, "segments");
      File outSegment = new File(testDir, "output");
      try {
        for (int i = 0; i < SEGMENT_CNT; i++) {
          File f = new File(dataDir, "seg" + i);
          nfs.mkdirs(f);
          createSegmentData(nfs, f, true);
        }
        runTool(dataDir, outSegment);
        SegmentReader sr = new SegmentReader(outSegment.listFiles()[0]);
        assertEquals(SEGMENT_CNT * PAGE_CNT, sr.size);
        sr.close();
      } catch (Throwable e) {
        e.printStackTrace();
        fail(e.getMessage() + ", " + e.getStackTrace());
      }
    } finally {
      nfs.close();
    }
  }
  
  protected void runTool(File dataDir, File outSegment) throws Exception {
    SegmentMergeTool.main(
            new String[] {"-dir", dataDir.toString(), "-o", outSegment.toString(),
                    "-ds"});
  }

  /**
   * Test merging segments with the same data. The output (merged segment)
   * should contain the number of pages equal exactly to the page count of a
   * single segment.
   *  
   */
  public void testSameMerge() throws IOException {
    NutchFileSystem nfs = new LocalFileSystem();
    try {
      File dataDir = new File(testDir, "segments");
      File outSegment = new File(testDir, "output");
      try {
        for (int i = 0; i < SEGMENT_CNT; i++) {
          File f = new File(dataDir, "seg" + i);
          nfs.mkdirs(f);
          createSegmentData(nfs, f, false);
        }
        runTool(dataDir, outSegment);
        SegmentReader sr = new SegmentReader(outSegment.listFiles()[0]);
        assertEquals(PAGE_CNT, sr.size);
        sr.close();
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    } catch (Throwable ex) {
      ex.printStackTrace();
      fail(ex.getMessage());
    } finally {
      nfs.close();
    }
  }

  public void testCorruptSegmentMerge() throws IOException {
    NutchFileSystem nfs = new LocalFileSystem();
    try {
      File dataDir = new File(testDir, "segments");
      File outSegment = new File(testDir, "output");
      try {
        for (int i = 0; i < SEGMENT_CNT; i++) {
          File f = new File(dataDir, "seg" + i);
          nfs.mkdirs(f);
          createSegmentData(nfs, f, true);
          // corrupt some segments in various ways... be creative :-)
          switch (i) {
            case 0:
              // truncate the fetcherOutput data file
              File data = new File(f, FetcherOutput.DIR_NAME);
              data = new File(data, "data");
              RandomAccessFile raf = new RandomAccessFile(data, "rws");
              raf.setLength(raf.length() - raf.length() / 4);
              raf.close();
              break;
            case 1:
              // truncate the Content data file
              data = new File(f, Content.DIR_NAME);
              data = new File(data, "data");
              raf = new RandomAccessFile(data, "rws");
              raf.setLength(raf.length() - raf.length() / 4);
              raf.close();
              break;
            case 2:
              // trash the whole
              // content
              data = new File(f, Content.DIR_NAME);
              new File(data, "data").delete();
              new File(data, "index").delete();
              break;
            case 3:
              // remove the "index" files - this is a very typical symptom for
              // segments created by a crashed fetcher process. Such segments should
              // be automatically fixed and recovered.
              data = new File(f, FetcherOutput.DIR_NAME);
              new File(data, "index").delete();
              data = new File(f, Content.DIR_NAME);
              new File(data, "index").delete();
              data = new File(f, ParseData.DIR_NAME);
              new File(data, "index").delete();
              data = new File(f, ParseText.DIR_NAME);
              new File(data, "index").delete();
              break;
            default:
              // do nothing
              ;
          }
        }
        runTool(dataDir, outSegment);
        SegmentReader sr = new SegmentReader(outSegment.listFiles()[0]);
        // we arrive at this expression as follows:
        // 1. SEGMENT_CNT - 1 : because we trash one whole segment
        // 2. 2 * PAGE_CNT / 4: because for two segments
        // we truncate 1/4 of the data file
        // 3. + 2: because sometimes truncation falls on
        // the boundary of the last entry
        int maxCnt = PAGE_CNT * (SEGMENT_CNT - 1) - 2 * PAGE_CNT / 4 + 2 * (SEGMENT_CNT -1);
        //System.out.println("maxCnt=" + maxCnt + ", sr.size=" + sr.size);
        assertTrue(sr.size < maxCnt);
        sr.close();
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    } catch (Throwable ex) {
      ex.printStackTrace();
      fail(ex.getMessage());
    } finally {
      nfs.close();
    }
  }
}

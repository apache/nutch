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

import java.lang.invoke.MethodHandles;
import java.text.DecimalFormat;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.MapFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * New SegmentMerger unit test focusing on several crappy issues with the
 * segment merger. The general problem is disappearing records and incorrect
 * CrawlDatum status values. This unit test performs random sequences of segment
 * merging where we're looking for an expected status. A second test is able to
 * randomly inject redirects in segment, likely causing the segment merger to
 * fail resulting in a bad merged segment.
 * 
 * See also:
 * 
 * https://issues.apache.org/jira/browse/NUTCH-1113
 * https://issues.apache.org/jira/browse/NUTCH-1616
 * https://issues.apache.org/jira/browse/NUTCH-1520
 * 
 * Cheers!
 */
public class TestSegmentMergerCrawlDatums {
  Configuration conf;
  FileSystem fs;
  Random rnd;

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  @Before
  public void setUp() throws Exception {
    conf = NutchConfiguration.create();
    fs = FileSystem.get(conf);
    rnd = new Random();
  }

  /**
   *
   */
  @Test
  public void testSingleRandomSequence() throws Exception {
    Assert.assertEquals(
        new Byte(CrawlDatum.STATUS_FETCH_SUCCESS),
        new Byte(executeSequence(CrawlDatum.STATUS_FETCH_GONE,
            CrawlDatum.STATUS_FETCH_SUCCESS, 256, false)));
  }

  /**
   *
   */
  @Test
  public void testMostlyRedirects() throws Exception {
    // Our test directory
    Path testDir = new Path(conf.get("hadoop.tmp.dir"), "merge-"
        + System.currentTimeMillis());

    Path segment1 = new Path(testDir, "20140110114943");
    Path segment2 = new Path(testDir, "20140110114832");
    Path segment3 = new Path(testDir, "20140110114558");
    Path segment4 = new Path(testDir, "20140110114930");
    Path segment5 = new Path(testDir, "20140110114545");
    Path segment6 = new Path(testDir, "20140110114507");
    Path segment7 = new Path(testDir, "20140110114903");
    Path segment8 = new Path(testDir, "20140110114724");

    createSegment(segment1, CrawlDatum.STATUS_FETCH_SUCCESS, true);
    createSegment(segment2, CrawlDatum.STATUS_FETCH_SUCCESS, true);
    createSegment(segment3, CrawlDatum.STATUS_FETCH_SUCCESS, true);
    createSegment(segment4, CrawlDatum.STATUS_FETCH_SUCCESS, true);
    createSegment(segment5, CrawlDatum.STATUS_FETCH_SUCCESS, true);
    createSegment(segment6, CrawlDatum.STATUS_FETCH_SUCCESS, false);
    createSegment(segment7, CrawlDatum.STATUS_FETCH_SUCCESS, true);
    createSegment(segment8, CrawlDatum.STATUS_FETCH_SUCCESS, true);

    // Merge the segments and get status
    Path mergedSegment = merge(testDir, new Path[] { segment1, segment2,
        segment3, segment4, segment5, segment6, segment7, segment8 });
    Byte status = new Byte(status = checkMergedSegment(testDir, mergedSegment));

    Assert.assertEquals(new Byte(CrawlDatum.STATUS_FETCH_SUCCESS), status);
  }

  /**
   *
   */
  @Test
  public void testRandomizedSequences() throws Exception {
    for (int i = 0; i < rnd.nextInt(16) + 16; i++) {
      byte expectedStatus = (byte) (rnd.nextInt(6) + 0x21);
      while (expectedStatus == CrawlDatum.STATUS_FETCH_RETRY
          || expectedStatus == CrawlDatum.STATUS_FETCH_NOTMODIFIED) {
        // fetch_retry and fetch_notmodified never remain in a merged segment
        expectedStatus = (byte) (rnd.nextInt(6) + 0x21);
      }
      byte randomStatus = (byte) (rnd.nextInt(6) + 0x21);
      int rounds = rnd.nextInt(16) + 32;
      boolean withRedirects = rnd.nextBoolean();

      byte resultStatus = executeSequence(randomStatus, expectedStatus, rounds,
          withRedirects);
      Assert.assertEquals(
          "Expected status = " + CrawlDatum.getStatusName(expectedStatus)
              + ", but got " + CrawlDatum.getStatusName(resultStatus)
              + " when merging " + rounds + " segments"
              + (withRedirects ? " with redirects" : ""), expectedStatus,
          resultStatus);
    }
  }

  /**
   *
   */
  @Test
  public void testRandomTestSequenceWithRedirects() throws Exception {
    Assert.assertEquals(
        new Byte(CrawlDatum.STATUS_FETCH_SUCCESS),
        new Byte(executeSequence(CrawlDatum.STATUS_FETCH_GONE,
            CrawlDatum.STATUS_FETCH_SUCCESS, 128, true)));
  }

  /**
   * Check a fixed sequence!
   */
  @Test
  public void testFixedSequence() throws Exception {
    // Our test directory
    Path testDir = new Path(conf.get("hadoop.tmp.dir"), "merge-"
        + System.currentTimeMillis());

    Path segment1 = new Path(testDir, "00001");
    Path segment2 = new Path(testDir, "00002");
    Path segment3 = new Path(testDir, "00003");

    createSegment(segment1, CrawlDatum.STATUS_FETCH_GONE, false);
    createSegment(segment2, CrawlDatum.STATUS_FETCH_GONE, true);
    createSegment(segment3, CrawlDatum.STATUS_FETCH_SUCCESS, false);

    // Merge the segments and get status
    Path mergedSegment = merge(testDir, new Path[] { segment1, segment2,
        segment3 });
    Byte status = new Byte(status = checkMergedSegment(testDir, mergedSegment));

    Assert.assertEquals(new Byte(CrawlDatum.STATUS_FETCH_SUCCESS), status);
  }

  /**
   * Check a fixed sequence!
   */
  @Test
  public void testRedirFetchInOneSegment() throws Exception {
    // Our test directory
    Path testDir = new Path(conf.get("hadoop.tmp.dir"), "merge-"
        + System.currentTimeMillis());

    Path segment = new Path(testDir, "00001");

    createSegment(segment, CrawlDatum.STATUS_FETCH_SUCCESS, true, true);

    // Merge the segments and get status
    Path mergedSegment = merge(testDir, new Path[] { segment });
    Byte status = new Byte(status = checkMergedSegment(testDir, mergedSegment));

    Assert.assertEquals(new Byte(CrawlDatum.STATUS_FETCH_SUCCESS), status);
  }

  /**
   * Check a fixed sequence!
   */
  @Test
  public void testEndsWithRedirect() throws Exception {
    // Our test directory
    Path testDir = new Path(conf.get("hadoop.tmp.dir"), "merge-"
        + System.currentTimeMillis());

    Path segment1 = new Path(testDir, "00001");
    Path segment2 = new Path(testDir, "00002");

    createSegment(segment1, CrawlDatum.STATUS_FETCH_SUCCESS, false);
    createSegment(segment2, CrawlDatum.STATUS_FETCH_SUCCESS, true);

    // Merge the segments and get status
    Path mergedSegment = merge(testDir, new Path[] { segment1, segment2 });
    Byte status = new Byte(status = checkMergedSegment(testDir, mergedSegment));

    Assert.assertEquals(new Byte(CrawlDatum.STATUS_FETCH_SUCCESS), status);
  }

  /**
   * Execute a sequence of creating segments, merging them and checking the
   * final output
   * 
   * @param status
   *          to start with
   * @param status
   *          to end with
   * @param number
   *          of rounds
   * @param whether
   *          redirects are injected randomly
   * @return the CrawlDatum status
   */
  protected byte executeSequence(byte firstStatus, byte lastStatus, int rounds,
      boolean redirect) throws Exception {
    // Our test directory
    Path testDir = new Path(conf.get("hadoop.tmp.dir"), "merge-"
        + System.currentTimeMillis());

    // Format for the segments
    DecimalFormat df = new DecimalFormat("0000000");

    // Create our segment paths
    Path[] segmentPaths = new Path[rounds];
    for (int i = 0; i < rounds; i++) {
      String segmentName = df.format(i);
      segmentPaths[i] = new Path(testDir, segmentName);
    }

    // Create the first segment according to the specified status
    createSegment(segmentPaths[0], firstStatus, false);

    // Create N segments with random status and optionally with randomized
    // redirect injection
    for (int i = 1; i < rounds - 1; i++) {
      // Status, 6 possibilities incremented with 33 hex
      byte status = (byte) (rnd.nextInt(6) + 0x21);

      // Whether this is going to be a redirect
      boolean addRedirect = redirect ? rnd.nextBoolean() : false;
      // If it's a redirect we add a datum resulting from a fetch at random,
      // if not: always add a fetch datum to avoid empty segments
      boolean addFetch = addRedirect ? rnd.nextBoolean() : true;

      createSegment(segmentPaths[i], status, addFetch, addRedirect);
    }

    // Create the last segment according to the specified status
    // (additionally, add a redirect at random)
    createSegment(segmentPaths[rounds - 1], lastStatus, true,
        redirect ? rnd.nextBoolean() : false);

    // Merge the segments!
    Path mergedSegment = merge(testDir, segmentPaths);

    // Check the status of the final record and return it
    return checkMergedSegment(testDir, mergedSegment);
  }

  /**
   * Checks the merged segment and removes the stuff again.
   * 
   * @param the
   *          test directory
   * @param the
   *          merged segment
   * @return the final status
   */
  protected byte checkMergedSegment(Path testDir, Path mergedSegment)
      throws Exception {
    // Get a MapFile reader for the <Text,CrawlDatum> pairs
    MapFile.Reader[] readers = MapFileOutputFormat.getReaders(fs, new Path(
        mergedSegment, CrawlDatum.FETCH_DIR_NAME), conf);

    Text key = new Text();
    CrawlDatum value = new CrawlDatum();
    byte finalStatus = 0x0;

    for (MapFile.Reader reader : readers) {
      while (reader.next(key, value)) {
        LOG.info("Reading status for: " + key.toString() + " > "
            + CrawlDatum.getStatusName(value.getStatus()));

        // Only consider fetch status
        if (CrawlDatum.hasFetchStatus(value)
            && key.toString().equals("http://nutch.apache.org/")) {
          finalStatus = value.getStatus();
        }
      }

      // Close the reader again
      reader.close();
    }

    // Remove the test directory again
    fs.delete(testDir, true);

    LOG.info("Final fetch status for: http://nutch.apache.org/ > "
        + CrawlDatum.getStatusName(finalStatus));

    // Return the final status
    return finalStatus;
  }

  /**
   * Merge some segments!
   * 
   * @param the
   *          test directory
   * @param the
   *          segments to merge
   * @return Path to the merged segment
   */
  protected Path merge(Path testDir, Path[] segments) throws Exception {
    // Our merged output directory
    Path out = new Path(testDir, "out");

    // Merge
    @SuppressWarnings("resource")
    SegmentMerger merger = new SegmentMerger(conf);
    merger.merge(out, segments, false, false, -1);

    FileStatus[] stats = fs.listStatus(out);
    Assert.assertEquals(2, stats.length);
    Assert.assertTrue("Only one merged directory should exist.", stats[0].isDirectory());
    Assert.assertTrue("One _SUCCESS file should exist.", !stats[1].isDirectory());

    return stats[0].getPath();
  }

  /**
   * Create a segment with the specified status.
   * 
   * @param the
   *          segment's paths
   * @param the
   *          status of the record, ignored if redirect is true
   * @param whether
   *          we're doing a redirect as well
   */
  protected void createSegment(Path segment, byte status, boolean redirect)
      throws Exception {
    if (redirect) {
      createSegment(segment, status, false, true);
    } else {
      createSegment(segment, status, true, false);
    }
  }

  protected void createSegment(Path segment, byte status, boolean fetch,
      boolean redirect) throws Exception {
    LOG.info("\nSegment: " + segment.toString());

    // The URL of our main record
    String url = "http://nutch.apache.org/";

    // The URL of our redirecting URL
    String redirectUrl = "http://nutch.apache.org/i_redirect_to_the_root/";

    // Our value
    CrawlDatum value = new CrawlDatum();

    // Path of the segment's crawl_fetch directory
    Path crawlFetchPath = new Path(
        new Path(segment, CrawlDatum.FETCH_DIR_NAME), "part-00000");

    // Get a writer for map files containing <Text,CrawlDatum> pairs
    Option wKeyOpt = MapFile.Writer.keyClass(Text.class);
    org.apache.hadoop.io.SequenceFile.Writer.Option wValueOpt = SequenceFile.Writer.valueClass(CrawlDatum.class);
    MapFile.Writer writer = new MapFile.Writer(conf, crawlFetchPath, wKeyOpt, wValueOpt);

    // Whether we're handling a redirect now
    // first add the linked datum
    // - before redirect status because url sorts before redirectUrl
    // - before fetch status to check whether fetch datum is preferred over
    // linked datum when merging
    if (redirect) {
      // We're writing our our main record URL with status linked
      LOG.info(url + " > " + CrawlDatum.getStatusName(CrawlDatum.STATUS_LINKED));
      value = new CrawlDatum();
      value.setStatus(CrawlDatum.STATUS_LINKED);
      writer.append(new Text(url), value);
    }

    // Whether we're fetching now
    if (fetch) {
      LOG.info(url + " > " + CrawlDatum.getStatusName(status));

      // Set the status
      value.setStatus(status);

      // Write the pair and ok
      writer.append(new Text(url), value);
    }

    // Whether we're handing a redirect now
    if (redirect) {
      // And the redirect URL with redirect status, pointing to our main URL
      LOG.info(redirectUrl + " > "
          + CrawlDatum.getStatusName(CrawlDatum.STATUS_FETCH_REDIR_TEMP));
      value.setStatus(CrawlDatum.STATUS_FETCH_REDIR_TEMP);
      writer.append(new Text(redirectUrl), value);
    }

    // Close the stuff
    writer.close();
  }

}

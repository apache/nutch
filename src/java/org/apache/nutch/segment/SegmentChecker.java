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
package org.apache.nutch.segment;

import java.io.IOException;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Check if the segment is valid */
public class SegmentChecker {

  private Path segmentPath;
  private FileSystem fs;

  public static final Logger LOG = LoggerFactory.getLogger(SegmentChecker.class);

  public SegmentChecker() {
    this.segmentPath = null;
    this.fs = null;
  }

  public SegmentChecker(Path segmentPath, FileSystem fs) {
    this.segmentPath = segmentPath;
    this.fs = fs;
  }

  public void set(Path segmentPath, FileSystem fs) {
    this.segmentPath = segmentPath;
    this.fs = fs;
  }

  public static boolean isIndexable(SegmentChecker segmentChecker) throws IOException {

    if (segmentChecker.segmentPath == null || segmentChecker.fs == null){
      LOG.info("No segment path or filesystem setted.");
      return false;
    }

    boolean checkResult = true;
    checkResult &= checkSegmentDir(segmentChecker);
    //Add new check methods here

    if (checkResult) {
      return true;
    } else {
      return false;
    }
  }

  /*
   * Check the segment to see if it is valid based on the sub directories. 
   */
  public static boolean checkSegmentDir(SegmentChecker segmentChecker) throws IOException {

    Path segmentPath = segmentChecker.segmentPath;
    FileSystem fs = segmentChecker.fs;

    FileStatus[] fstats_segment = fs.listStatus(segmentPath, HadoopFSUtil.getPassDirectoriesFilter(fs));
    Path[] segment_files = HadoopFSUtil.getPaths(fstats_segment);

    boolean isCrawlFetchExisted = false;
    boolean isCrawlParseExisted = false;
    boolean isParseDataExisted  = false;
    boolean isParseTextExisted  = false;

    for (Path path : segment_files) {
      String pathName = path.getName();
      isCrawlFetchExisted |= pathName.equals(CrawlDatum.FETCH_DIR_NAME);
      isCrawlParseExisted |= pathName.equals(CrawlDatum.PARSE_DIR_NAME);
      isParseDataExisted |= pathName.equals(ParseData.DIR_NAME);
      isParseTextExisted |= pathName.equals(ParseText.DIR_NAME);
    }

    if (isParseTextExisted && isCrawlParseExisted && isCrawlFetchExisted
        && isParseDataExisted) {

      LOG.info("Segment dir is complete: " + segmentPath.toString() + ".");
      return true;
    } else {

      //log the missing dir
      StringBuilder missingDir = new StringBuilder("");
      if (isParseDataExisted == false) {
        missingDir.append(ParseData.DIR_NAME + ", ");
      }
      if (isParseTextExisted == false) {
        missingDir.append(ParseText.DIR_NAME + ", ");
      }
      if (isCrawlParseExisted == false) {
        missingDir.append(CrawlDatum.PARSE_DIR_NAME + ", ");
      }
      if (isCrawlFetchExisted == false) {
        missingDir.append(CrawlDatum.FETCH_DIR_NAME + ", ");
      }

      String missingDirString = missingDir.toString();
      LOG.warn("Skipping segment: " + segmentPath.toString()+ ". Missing sub directories: "
          + missingDirString.substring(0, missingDirString.length() - 2));

      return false;
    }
  }

}
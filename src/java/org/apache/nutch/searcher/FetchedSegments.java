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

package org.apache.nutch.searcher;

import java.io.IOException;
import java.io.File;

import java.util.HashMap;

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.db.*;
import org.apache.nutch.util.*;
import org.apache.nutch.fetcher.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.pagedb.*;
import org.apache.nutch.indexer.*;

/** Implements {@link HitSummarizer} and {@link HitContent} for a set of
 * fetched segments. */
public class FetchedSegments implements HitSummarizer, HitContent {

  private static class Segment {
    private NutchFileSystem nfs;
    private File segmentDir;

    private ArrayFile.Reader fetcher;
    private ArrayFile.Reader content;
    private ArrayFile.Reader text;
    private ArrayFile.Reader parsedata;

    public Segment(NutchFileSystem nfs, File segmentDir) throws IOException {
      this.nfs = nfs;
      this.segmentDir = segmentDir;
    }

    public FetcherOutput getFetcherOutput(int docNo) throws IOException {
      if (fetcher == null) { 
        this.fetcher = new ArrayFile.Reader
          (nfs, new File(segmentDir, FetcherOutput.DIR_NAME).toString());
      }

      FetcherOutput entry = new FetcherOutput();
      fetcher.get(docNo, entry);
      return entry;
    }

    public byte[] getContent(int docNo) throws IOException {
      if (content == null) {
        this.content = new ArrayFile.Reader
          (nfs, new File(segmentDir, Content.DIR_NAME).toString());
      }

      Content entry = new Content();
      content.get(docNo, entry);
      return entry.getContent();
    }

    public ParseData getParseData(int docNo) throws IOException {
      if (parsedata == null) {
        this.parsedata = new ArrayFile.Reader
          (nfs, new File(segmentDir, ParseData.DIR_NAME).toString());
      }
      
      ParseData entry = new ParseData();
      parsedata.get(docNo, entry);
      return entry;
    }

    public ParseText getParseText(int docNo) throws IOException {
      if (text == null) {
        this.text = new ArrayFile.Reader
          (nfs, new File(segmentDir, ParseText.DIR_NAME).toString());
      }

      ParseText entry = new ParseText();
      text.get(docNo, entry);
      return entry;
    }
    
  }

  private HashMap segments = new HashMap();

  /** Construct given a directory containing fetcher output. */
  public FetchedSegments(NutchFileSystem nfs, String segmentsDir) throws IOException {
    File[] segmentDirs = nfs.listFiles(new File(segmentsDir));

    if (segmentDirs != null) {
        for (int i = 0; i < segmentDirs.length; i++) {
            File segmentDir = segmentDirs[i];
            File indexdone = new File(segmentDir, IndexSegment.DONE_NAME);
            if (nfs.exists(indexdone) && nfs.isFile(indexdone)) {
            	segments.put(segmentDir.getName(), new Segment(nfs, segmentDir));
            }
        }
    }
  }

  public String[] getSegmentNames() {
    return (String[])segments.keySet().toArray(new String[segments.size()]);
  }

  public byte[] getContent(HitDetails details) throws IOException {
    return getSegment(details).getContent(getDocNo(details));
  }

  public ParseData getParseData(HitDetails details) throws IOException {
    return getSegment(details).getParseData(getDocNo(details));
  }

  public String[] getAnchors(HitDetails details) throws IOException {
    return getSegment(details).getFetcherOutput(getDocNo(details))
      .getFetchListEntry().getAnchors();
  }

  public long getFetchDate(HitDetails details) throws IOException {
    return getSegment(details).getFetcherOutput(getDocNo(details))
      .getFetchDate();
  }

  public ParseText getParseText(HitDetails details) throws IOException {
    return getSegment(details).getParseText(getDocNo(details));
  }

  public String getSummary(HitDetails details, Query query)
    throws IOException {

    String text = getSegment(details).getParseText(getDocNo(details)).getText();

    return new Summarizer().getSummary(text, query).toString();
  }
    
  public String[] getSummary(HitDetails[] details, Query query)
    throws IOException {
    String[] results = new String[details.length];
    for (int i = 0; i < details.length; i++)
      results[i] = getSummary(details[i], query);
    return results;
  }


  private Segment getSegment(HitDetails details) {
    return (Segment)segments.get(details.getValue("segment"));
  }

  private int getDocNo(HitDetails details) {
    return Integer.parseInt(details.getValue("docNo"), 16);
  }


}

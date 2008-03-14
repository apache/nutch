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

package org.apache.nutch.searcher;

import java.io.IOException;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.parse.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.nutch.crawl.*;

/** Implements {@link HitSummarizer} and {@link HitContent} for a set of
 * fetched segments. */
public class FetchedSegments implements HitSummarizer, HitContent {

  private static class Segment implements Closeable {
    
    private static final Partitioner PARTITIONER = new HashPartitioner();

    private FileSystem fs;
    private Path segmentDir;

    private MapFile.Reader[] content;
    private MapFile.Reader[] parseText;
    private MapFile.Reader[] parseData;
    private MapFile.Reader[] crawl;
    private Configuration conf;

    public Segment(FileSystem fs, Path segmentDir, Configuration conf) throws IOException {
      this.fs = fs;
      this.segmentDir = segmentDir;
      this.conf = conf;
    }

    public CrawlDatum getCrawlDatum(Text url) throws IOException {
      synchronized (this) {
        if (crawl == null)
          crawl = getReaders(CrawlDatum.FETCH_DIR_NAME);
      }
      return (CrawlDatum)getEntry(crawl, url, new CrawlDatum());
    }
    
    public byte[] getContent(Text url) throws IOException {
      synchronized (this) {
        if (content == null)
          content = getReaders(Content.DIR_NAME);
      }
      return ((Content)getEntry(content, url, new Content())).getContent();
    }

    public ParseData getParseData(Text url) throws IOException {
      synchronized (this) {
        if (parseData == null)
          parseData = getReaders(ParseData.DIR_NAME);
      }
      return (ParseData)getEntry(parseData, url, new ParseData());
    }

    public ParseText getParseText(Text url) throws IOException {
      synchronized (this) {
        if (parseText == null)
          parseText = getReaders(ParseText.DIR_NAME);
      }
      return (ParseText)getEntry(parseText, url, new ParseText());
    }
    
    private MapFile.Reader[] getReaders(String subDir) throws IOException {
      return MapFileOutputFormat.getReaders(fs, new Path(segmentDir, subDir), this.conf);
    }

    private Writable getEntry(MapFile.Reader[] readers, Text url,
                              Writable entry) throws IOException {
      return MapFileOutputFormat.getEntry(readers, PARTITIONER, url, entry);
    }

    public void close() throws IOException {
      if (content != null) { closeReaders(content); }
      if (parseText != null) { closeReaders(parseText); }
      if (parseData != null) { closeReaders(parseData); }
      if (crawl != null) { closeReaders(crawl); }
    }

    private void closeReaders(MapFile.Reader[] readers) throws IOException {
      for (int i = 0; i < readers.length; i++) {
        readers[i].close();
      }
    }

  }

  private HashMap segments = new HashMap();
  private Summarizer summarizer;

  /** Construct given a directory containing fetcher output. */
  public FetchedSegments(FileSystem fs, String segmentsDir, Configuration conf) throws IOException {
    Path[] segmentDirs = fs.listPaths(new Path(segmentsDir));
    this.summarizer = new SummarizerFactory(conf).getSummarizer();

    if (segmentDirs != null) {
        for (int i = 0; i < segmentDirs.length; i++) {
            Path segmentDir = segmentDirs[i];
//             Path indexdone = new Path(segmentDir, IndexSegment.DONE_NAME);
//             if (fs.exists(indexdone) && fs.isFile(indexdone)) {
//             	segments.put(segmentDir.getName(), new Segment(fs, segmentDir));
//             }
            segments.put(segmentDir.getName(), new Segment(fs, segmentDir, conf));

        }
    }
  }

  public String[] getSegmentNames() {
    return (String[])segments.keySet().toArray(new String[segments.size()]);
  }

  public byte[] getContent(HitDetails details) throws IOException {
    return getSegment(details).getContent(getUrl(details));
  }

  public ParseData getParseData(HitDetails details) throws IOException {
    return getSegment(details).getParseData(getUrl(details));
  }

  public long getFetchDate(HitDetails details) throws IOException {
    return getSegment(details).getCrawlDatum(getUrl(details))
      .getFetchTime();
  }

  public ParseText getParseText(HitDetails details) throws IOException {
    return getSegment(details).getParseText(getUrl(details));
  }

  public Summary getSummary(HitDetails details, Query query)
    throws IOException {
    
    if (this.summarizer == null) { return new Summary(); }
    
    Segment segment = getSegment(details);
    ParseText parseText = segment.getParseText(getUrl(details));
    String text = (parseText != null) ? parseText.getText() : "";
    
    return this.summarizer.getSummary(text, query);
  }
    
  private class SummaryThread extends Thread {
    private HitDetails details;
    private Query query;

    private Summary summary;
    private Throwable throwable;

    public SummaryThread(HitDetails details, Query query) {
      this.details = details;
      this.query = query;
    }

    public void run() {
      try {
        this.summary = getSummary(details, query);
      } catch (Throwable throwable) {
        this.throwable = throwable;
      }
    }

  }


  public Summary[] getSummary(HitDetails[] details, Query query)
    throws IOException {
    SummaryThread[] threads = new SummaryThread[details.length];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new SummaryThread(details[i], query);
      threads[i].start();
    }

    Summary[] results = new Summary[details.length];
    for (int i = 0; i < threads.length; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      if (threads[i].throwable instanceof IOException) {
        throw (IOException)threads[i].throwable;
      } else if (threads[i].throwable != null) {
        throw new RuntimeException(threads[i].throwable);
      }
      results[i] = threads[i].summary;
    }
    return results;
  }


  private Segment getSegment(HitDetails details) {
    return (Segment)segments.get(details.getValue("segment"));
  }

  private Text getUrl(HitDetails details) {
    String url = details.getValue("orig");
    if (StringUtils.isBlank(url)) {
      url = details.getValue("url");
    }
    return new Text(url);
  }

  public void close() throws IOException {
    Iterator iterator = segments.values().iterator();
    while (iterator.hasNext()) {
      ((Segment) iterator.next()).close();
    }
  }
  
}

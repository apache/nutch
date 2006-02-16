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
import java.util.Iterator;

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
    private File segmentDir;

    private MapFile.Reader[] content;
    private MapFile.Reader[] parseText;
    private MapFile.Reader[] parseData;
    private MapFile.Reader[] crawl;
    private Configuration conf;

    public Segment(FileSystem fs, File segmentDir, Configuration conf) throws IOException {
      this.fs = fs;
      this.segmentDir = segmentDir;
      this.conf = conf;
    }

    public CrawlDatum getCrawlDatum(UTF8 url) throws IOException {
      synchronized (this) {
        if (crawl == null)
          crawl = getReaders(CrawlDatum.FETCH_DIR_NAME);
      }
      return (CrawlDatum)getEntry(crawl, url, new CrawlDatum());
    }
    
    public byte[] getContent(UTF8 url) throws IOException {
      synchronized (this) {
        if (content == null)
          content = getReaders(Content.DIR_NAME);
      }
      return ((Content)getEntry(content, url, new Content())).getContent();
    }

    public ParseData getParseData(UTF8 url) throws IOException {
      synchronized (this) {
        if (parseData == null)
          parseData = getReaders(ParseData.DIR_NAME);
      }
      return (ParseData)getEntry(parseData, url, new ParseData());
    }

    public ParseText getParseText(UTF8 url) throws IOException {
      synchronized (this) {
        if (parseText == null)
          parseText = getReaders(ParseText.DIR_NAME);
      }
      return (ParseText)getEntry(parseText, url, new ParseText());
    }
    
    private MapFile.Reader[] getReaders(String subDir) throws IOException {
      return MapFileOutputFormat.getReaders(fs, new File(segmentDir, subDir), this.conf);
    }

    private Writable getEntry(MapFile.Reader[] readers, UTF8 url,
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
  private int sumContext = 5;
  private int sumLength = 20;
  private Summarizer summarizer;

  /** Construct given a directory containing fetcher output. */
  public FetchedSegments(FileSystem fs, String segmentsDir, Configuration conf) throws IOException {
    File[] segmentDirs = fs.listFiles(new File(segmentsDir));
    this.sumContext = conf.getInt("searcher.summary.context", 5);
    this.sumLength = conf.getInt("searcher.summary.length", 20);
    this.summarizer = new Summarizer(conf);

    if (segmentDirs != null) {
        for (int i = 0; i < segmentDirs.length; i++) {
            File segmentDir = segmentDirs[i];
//             File indexdone = new File(segmentDir, IndexSegment.DONE_NAME);
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

  public String getSummary(HitDetails details, Query query)
    throws IOException {

    String text = getSegment(details).getParseText(getUrl(details)).getText();

    return this.summarizer.getSummary(text, query, this.sumContext, this.sumLength).toString();
  }
    
  private class SummaryThread extends Thread {
    private HitDetails details;
    private Query query;

    private String summary;
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


  public String[] getSummary(HitDetails[] details, Query query)
    throws IOException {
    SummaryThread[] threads = new SummaryThread[details.length];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new SummaryThread(details[i], query);
      threads[i].start();
    }

    String[] results = new String[details.length];
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

  private UTF8 getUrl(HitDetails details) {
    return new UTF8(details.getValue("url"));
  }

  public void close() throws IOException {
    Iterator iterator = segments.values().iterator();
    while (iterator.hasNext()) {
      ((Segment) iterator.next()).close();
    }
  }
  
}

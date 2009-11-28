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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.nutch.crawl.*;

/** Implements {@link HitSummarizer} and {@link HitContent} for a set of
 * fetched segments. */
public class FetchedSegments implements RPCSegmentBean {

  public static final long VERSION = 1L;

  private static final ExecutorService executor =
    Executors.newCachedThreadPool();

  private class SummaryTask implements Callable<Summary> {
    private final HitDetails details;
    private final Query query;

    public SummaryTask(HitDetails details, Query query) {
      this.details = details;
      this.query = query;
    }

    public Summary call() throws Exception {
      return getSummary(details, query);
    }
  }

  private class SegmentUpdater extends Thread {

    private volatile boolean stopRequested = false;

    @Override
    public void interrupt() {
      super.interrupt();
      stopRequested = true;
    }


    @Override
    public void run() {

      while (!stopRequested && !Thread.currentThread().isInterrupted()) {
        try {
          final FileStatus[] fstats = fs.listStatus(segmentsDir,
              HadoopFSUtil.getPassDirectoriesFilter(fs));
          final Path[] segmentDirs = HadoopFSUtil.getPaths(fstats);
          final Iterator<Map.Entry<String, Segment>> i =
            segments.entrySet().iterator();
          while (i.hasNext()) {
            final Map.Entry<String, Segment> entry = i.next();
            final Segment seg = entry.getValue();
            if (!fs.exists(seg.segmentDir)) {
              try {
                seg.close();
              } catch (final Exception e) {
                /* A segment may fail to close
                 * since it may already be deleted from
                 * file system. So we just ignore the
                 * exception and remove the mapping from
                 * 'segments'.
                 */
              } finally {
                i.remove();
              }
            }
          }

          if (segmentDirs != null) {
            for (final Path segmentDir : segmentDirs) {
              segments.putIfAbsent(segmentDir.getName(),
                  new Segment(fs, segmentDir, conf));
            }
          }

          Thread.sleep(60000);
        } catch (final InterruptedException e) {
          // ignore
        } catch (final IOException e) {
          // ignore
        }
      }
    }

  }


  private static class Segment implements java.io.Closeable {

    private static final Partitioner<Text, Writable> PARTITIONER =
      new HashPartitioner<Text, Writable>();

    private final FileSystem fs;
    private final Path segmentDir;

    private MapFile.Reader[] content;
    private MapFile.Reader[] parseText;
    private MapFile.Reader[] parseData;
    private MapFile.Reader[] crawl;
    private final Configuration conf;

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

  private final ConcurrentMap<String, Segment> segments =
    new ConcurrentHashMap<String, Segment>();
  private final FileSystem fs;
  private final Configuration conf;
  private final Path segmentsDir;
  
  // This must be nullable upon close, so do not declare final.
  private SegmentUpdater segUpdater;
  private final Summarizer summarizer;

  /** Construct given a directory containing fetcher output. */
  public FetchedSegments(Configuration conf, Path segmentsDir)
  throws IOException {
    this.conf = conf;
    this.fs = FileSystem.get(this.conf);
    final FileStatus[] fstats = fs.listStatus(segmentsDir,
        HadoopFSUtil.getPassDirectoriesFilter(fs));
    final Path[] segmentDirs = HadoopFSUtil.getPaths(fstats);
    this.summarizer = new SummarizerFactory(this.conf).getSummarizer();
    this.segmentsDir = segmentsDir;
    this.segUpdater = new SegmentUpdater();

    if (segmentDirs != null) {
      for (final Path segmentDir : segmentDirs) {
        segments.put(segmentDir.getName(),
          new Segment(this.fs, segmentDir, this.conf));
      }
    }
    this.segUpdater.start();
  }

  public String[] getSegmentNames() {
    return segments.keySet().toArray(new String[segments.size()]);
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

    final Segment segment = getSegment(details);
    final ParseText parseText = segment.getParseText(getUrl(details));
    final String text = (parseText != null) ? parseText.getText() : "";

    return this.summarizer.getSummary(text, query);
  }

  public long getProtocolVersion(String protocol, long clientVersion)
  throws IOException {
    return VERSION;
  }

  public Summary[] getSummary(HitDetails[] details, Query query)
    throws IOException {
    final List<Callable<Summary>> tasks =
      new ArrayList<Callable<Summary>>(details.length);
    for (int i = 0; i < details.length; i++) {
      tasks.add(new SummaryTask(details[i], query));
    }

    List<Future<Summary>> summaries;
    try {
      summaries = executor.invokeAll(tasks);
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }


    final Summary[] results = new Summary[details.length];
    for (int i = 0; i < details.length; i++) {
      final Future<Summary> f = summaries.get(i);
      Summary summary;
      try {
        summary = f.get();
      } catch (final Exception e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        throw new RuntimeException(e);
      }
      results[i] = summary;
    }
    return results;
  }


  private Segment getSegment(HitDetails details) {
    return segments.get(details.getValue("segment"));
  }

  private Text getUrl(HitDetails details) {
    String url = details.getValue("orig");
    if (StringUtils.isBlank(url)) {
      url = details.getValue("url");
    }
    return new Text(url);
  }

  public void close() throws IOException {
    // Interrupt that thread to convince it to stop running.
    segUpdater.interrupt();

    // Break reference cycle, otherwise this points to segUpdater, and 
    // segUpdater.$0 points to this.  It appeared to keep the thread from
    // being GC'ed/reaped.
    segUpdater = null;
    final Iterator<Segment> iterator = segments.values().iterator();
    while (iterator.hasNext()) {
      iterator.next().close();
    }
  }

}

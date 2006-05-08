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

package org.apache.nutch.fetcher;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.mapred.*;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.util.*;

import java.util.logging.*;

/** The fetcher. Most of the work is done by plugins. */
public class Fetcher extends Configured implements MapRunnable { 

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.fetcher.Fetcher");
  
  public static final String SIGNATURE_KEY = "nutch.content.digest";
  public static final String SEGMENT_NAME_KEY = "nutch.segment.name";
  public static final String SCORE_KEY = "nutch.crawl.score";

  public static class InputFormat extends SequenceFileInputFormat {
    /** Don't split inputs, to keep things polite. */
    public FileSplit[] getSplits(FileSystem fs, JobConf job, int nSplits)
      throws IOException {
      Path[] files = listPaths(fs, job);
      FileSplit[] splits = new FileSplit[files.length];
      for (int i = 0; i < files.length; i++) {
        splits[i] = new FileSplit(files[i], 0, fs.getLength(files[i]));
      }
      return splits;
    }
  }

  private RecordReader input;
  private OutputCollector output;
  private Reporter reporter;

  private String segmentName;
  private int activeThreads;
  private int maxRedirect;

  private long start = System.currentTimeMillis(); // start time of fetcher run
  private long lastRequestStart = start;

  private long bytes;                             // total bytes fetched
  private int pages;                              // total pages fetched
  private int errors;                             // total pages errored

  private boolean storingContent;
  private boolean parsing;

  private class FetcherThread extends Thread {
    private Configuration conf;
    private URLFilters urlFilters;
    private ParseUtil parseUtil;
    private UrlNormalizer normalizer;
    private ProtocolFactory protocolFactory;

    public FetcherThread(Configuration conf) {
      this.setDaemon(true);                       // don't hang JVM on exit
      this.setName("FetcherThread");              // use an informative name
      this.conf = conf;
      this.urlFilters = new URLFilters(conf);
      this.parseUtil = new ParseUtil(conf);
      this.protocolFactory = new ProtocolFactory(conf);
      this.normalizer = new UrlNormalizerFactory(conf).getNormalizer();
    }

    public void run() {
      synchronized (Fetcher.this) {activeThreads++;} // count threads
      
      try {
        UTF8 key = new UTF8();
        CrawlDatum datum = new CrawlDatum();
        
        while (true) {
          if (LogFormatter.hasLoggedSevere())     // something bad happened
            break;                                // exit
          
          try {                                   // get next entry from input
            if (!input.next(key, datum)) {
              break;                              // at eof, exit
            }
          } catch (IOException e) {
            e.printStackTrace();
            LOG.severe("fetcher caught:"+e.toString());
            break;
          }

          synchronized (Fetcher.this) {
            lastRequestStart = System.currentTimeMillis();
          }

          // url may be changed through redirects.
          UTF8 url = new UTF8();
          url.set(key);
          try {
            LOG.info("fetching " + url);            // fetch the page
            
            boolean redirecting;
            int redirectCount = 0;
            do {
              redirecting = false;
              LOG.fine("redirectCount=" + redirectCount);
              Protocol protocol = this.protocolFactory.getProtocol(url.toString());
              ProtocolOutput output = protocol.getProtocolOutput(url, datum);
              ProtocolStatus status = output.getStatus();
              Content content = output.getContent();
              ParseStatus pstatus = null;

              switch(status.getCode()) {

              case ProtocolStatus.SUCCESS:        // got a page
                pstatus = output(url, datum, content, CrawlDatum.STATUS_FETCH_SUCCESS);
                updateStatus(content.getContent().length);
                if (pstatus != null && pstatus.isSuccess() &&
                        pstatus.getMinorCode() == ParseStatus.SUCCESS_REDIRECT) {
                  String newUrl = pstatus.getMessage();
                  newUrl = normalizer.normalize(newUrl);
                  newUrl = this.urlFilters.filter(newUrl);
                  if (newUrl != null && !newUrl.equals(url.toString())) {
                    url = new UTF8(newUrl);
                    redirecting = true;
                    redirectCount++;
                    LOG.fine(" - content redirect to " + url);
                  } else {
                    LOG.fine(" - content redirect skipped: " +
                             (newUrl != null ? "to same url" : "filtered"));
                  }
                }
                break;

              case ProtocolStatus.MOVED:         // redirect
              case ProtocolStatus.TEMP_MOVED:
                String newUrl = status.getMessage();
                newUrl = normalizer.normalize(newUrl);
                newUrl = this.urlFilters.filter(newUrl);
                if (newUrl != null && !newUrl.equals(url.toString())) {
                  url = new UTF8(newUrl);
                  redirecting = true;
                  redirectCount++;
                  LOG.fine(" - protocol redirect to " + url);
                } else {
                  LOG.fine(" - protocol redirect skipped: " +
                           (newUrl != null ? "to same url" : "filtered"));
                }
                break;

              case ProtocolStatus.EXCEPTION:
                logError(url, status.getMessage());
              case ProtocolStatus.RETRY:          // retry
                datum.setRetriesSinceFetch(datum.getRetriesSinceFetch()+1);
                output(url, datum, null, CrawlDatum.STATUS_FETCH_RETRY);
                break;
                
              case ProtocolStatus.GONE:           // gone
              case ProtocolStatus.NOTFOUND:
              case ProtocolStatus.ACCESS_DENIED:
              case ProtocolStatus.ROBOTS_DENIED:
              case ProtocolStatus.NOTMODIFIED:
                output(url, datum, null, CrawlDatum.STATUS_FETCH_GONE);
                break;

              default:
                LOG.warning("Unknown ProtocolStatus: " + status.getCode());
                output(url, datum, null, CrawlDatum.STATUS_FETCH_GONE);
              }

              if (redirecting && redirectCount >= maxRedirect) {
                LOG.info(" - redirect count exceeded " + url);
                output(url, datum, null, CrawlDatum.STATUS_FETCH_GONE);
              }

            } while (redirecting && (redirectCount < maxRedirect));

            
          } catch (Throwable t) {                 // unexpected exception
            logError(url, t.toString());
            output(url, datum, null, CrawlDatum.STATUS_FETCH_GONE);
            
          }
        }

      } catch (Throwable e) {
        e.printStackTrace();
        LOG.severe("fetcher caught:"+e.toString());
      } finally {
        synchronized (Fetcher.this) {activeThreads--;} // count threads
      }
    }

    private void logError(UTF8 url, String message) {
      LOG.info("fetch of " + url + " failed with: " + message);
      synchronized (Fetcher.this) {               // record failure
        errors++;
      }
    }

    private ParseStatus output(UTF8 key, CrawlDatum datum,
                        Content content, int status) {

      datum.setStatus(status);
      datum.setFetchTime(System.currentTimeMillis());

      if (content == null) {
        String url = key.toString();
        content = new Content(url, url, new byte[0], "", new Metadata(), this.conf);
      }
      Metadata metadata = content.getMetadata();
      // add segment to metadata
      metadata.set(SEGMENT_NAME_KEY, segmentName);
      // add score to metadata
      metadata.set(SCORE_KEY, Float.toString(datum.getScore()));

      Parse parse = null;
      if (parsing && status == CrawlDatum.STATUS_FETCH_SUCCESS) {
        ParseStatus parseStatus;
        try {
          parse = this.parseUtil.parse(content);
          parseStatus = parse.getData().getStatus();
        } catch (Exception e) {
          parseStatus = new ParseStatus(e);
        }
        if (!parseStatus.isSuccess()) {
          LOG.warning("Error parsing: " + key + ": " + parseStatus);
          parse = parseStatus.getEmptyParse(getConf());
        }
        // Calculate page signature. For non-parsing fetchers this will
        // be done in ParseSegment
        byte[] signature = SignatureFactory.getSignature(getConf()).calculate(content, parse);
        metadata.set(SIGNATURE_KEY, StringUtil.toHexString(signature));
        datum.setSignature(signature);
        // Ensure segment name and score are in parseData metadata
        parse.getData().getContentMeta().set(SEGMENT_NAME_KEY, segmentName); 	 
        parse.getData().getContentMeta().set(SCORE_KEY, Float.toString(datum.getScore()));
        parse.getData().getContentMeta().set(SIGNATURE_KEY, StringUtil.toHexString(signature));
      }

      try {
        output.collect
          (key,
           new FetcherOutput(datum,
                             storingContent ? content : null,
                             parse != null ? new ParseImpl(parse) : null));
      } catch (IOException e) {
        e.printStackTrace();
        LOG.severe("fetcher caught:"+e.toString());
      }
      if (parse != null) return parse.getData().getStatus();
      else return null;
    }
    
  }

  public Fetcher() { super(null); }

  public Fetcher(Configuration conf) { super(conf); }

  private synchronized void updateStatus(int bytesInPage) throws IOException {
    pages++;
    bytes += bytesInPage;
  }

  private void reportStatus() throws IOException {
    String status;
    synchronized (this) {
      long elapsed = (System.currentTimeMillis() - start)/1000;
      status = 
        pages+" pages, "+errors+" errors, "
        + Math.round(((float)pages*10)/elapsed)/10.0+" pages/s, "
        + Math.round(((((float)bytes)*8)/1024)/elapsed)+" kb/s, ";
    }
    reporter.setStatus(status);
  }

  public void configure(JobConf job) {
    setConf(job);

    this.segmentName = job.get(SEGMENT_NAME_KEY);
    this.storingContent = isStoringContent(job);
    this.parsing = isParsing(job);

    if (job.getBoolean("fetcher.verbose", false)) {
      LOG.setLevel(Level.FINE);
    }
  }

  public void close() {}

  public static boolean isParsing(Configuration conf) {
    return conf.getBoolean("fetcher.parse", true);
  }

  public static boolean isStoringContent(Configuration conf) {
    return conf.getBoolean("fetcher.store.content", true);
  }

  public void run(RecordReader input, OutputCollector output,
                  Reporter reporter) throws IOException {

    this.input = input;
    this.output = output;
    this.reporter = reporter;

    this.maxRedirect = getConf().getInt("http.redirect.max", 3);
    
    int threadCount = getConf().getInt("fetcher.threads.fetch", 10);
    LOG.info("Fetcher: threads: " + threadCount);

    for (int i = 0; i < threadCount; i++) {       // spawn threads
      new FetcherThread(getConf()).start();
    }

    // select a timeout that avoids a task timeout
    long timeout = getConf().getInt("mapred.task.timeout", 10*60*1000)/2;

    do {                                          // wait for threads to exit
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}

      reportStatus();

      // some requests seem to hang, despite all intentions
      synchronized (this) {
        if ((System.currentTimeMillis() - lastRequestStart) > timeout) { 
          LOG.warning("Aborting with "+activeThreads+" hung threads.");
          return;
        }
      }

    } while (activeThreads > 0);
    
  }

  public void fetch(Path segment, int threads, boolean parsing)
    throws IOException {

    LOG.info("Fetcher: starting");
    LOG.info("Fetcher: segment: " + segment);

    JobConf job = new NutchJob(getConf());
    job.setJobName("fetch " + segment);

    job.setInt("fetcher.threads.fetch", threads);
    job.set(SEGMENT_NAME_KEY, segment.getName());
    job.setBoolean("fetcher.parse", parsing);

    // for politeness, don't permit parallel execution of a single task
    job.setBoolean("mapred.speculative.execution", false);

    job.setInputPath(new Path(segment, CrawlDatum.GENERATE_DIR_NAME));
    job.setInputFormat(InputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(CrawlDatum.class);

    job.setMapRunnerClass(Fetcher.class);

    job.setOutputPath(segment);
    job.setOutputFormat(FetcherOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(FetcherOutput.class);

    JobClient.runJob(job);
    LOG.info("Fetcher: done");
  }


  /** Run the fetcher. */
  public static void main(String[] args) throws Exception {

    String usage = "Usage: Fetcher <segment> [-threads n] [-noParsing]";

    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }
      
    Path segment = new Path(args[0]);

    Configuration conf = NutchConfiguration.create();

    int threads = conf.getInt("fetcher.threads.fetch", 10);
    boolean parsing = true;

    for (int i = 1; i < args.length; i++) {       // parse command line
      if (args[i].equals("-threads")) {           // found -threads option
        threads =  Integer.parseInt(args[++i]);
      } else if (args[i].equals("-noParsing")) parsing = false;
    }

    conf.setInt("fetcher.threads.fetch", threads);
    if (!parsing) {
      conf.setBoolean("fetcher.parse", parsing);
    }
    Fetcher fetcher = new Fetcher(conf);          // make a Fetcher
    
    fetcher.fetch(segment, threads, parsing);              // run the Fetcher

  }
}

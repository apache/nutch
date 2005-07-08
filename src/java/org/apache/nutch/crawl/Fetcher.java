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

package org.apache.nutch.crawl;

import java.io.IOException;
import java.io.File;
import java.util.Properties;

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.net.*;
import org.apache.nutch.util.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.mapred.*;

import java.util.logging.*;

/** The fetcher. Most of the work is done by plugins. */
public class Fetcher extends NutchConfigured implements MapRunnable { 

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.fetcher.Fetcher");
  
  public static final String DIGEST_KEY = "nutch.content.digest";

  private RecordReader input;
  private OutputCollector output;

  private int activeThreads;
  private int maxRedirect;

  private long start = System.currentTimeMillis(); // start time of fetcher run

  private long bytes;                             // total bytes fetched
  private int pages;                              // total pages fetched
  private int errors;                             // total pages errored

  private class FetcherThread extends Thread {
    public void run() {
      synchronized (Fetcher.this) {activeThreads++;} // count threads
      
      try {
        UTF8 key = new UTF8();
        CrawlDatum datum = new CrawlDatum();
        
        while (true) {
          if (LogFormatter.hasLoggedSevere())     // something bad happened
            break;                                // exit
          
          try {                                   // get next entry from input
            if (!input.next(key, datum))
              break;                              // at eof, exit
          } catch (IOException e) {
            LOG.severe("fetcher caught:"+e.toString());
            break;
          }

          String url = key.toString();
          try {
            LOG.info("fetching " + url);            // fetch the page
            
            boolean redirecting;
            int redirectCount = 0;
            do {
              redirecting = false;
              LOG.fine("redirectCount=" + redirectCount);
              Protocol protocol = ProtocolFactory.getProtocol(url);
              ProtocolOutput output = protocol.getProtocolOutput(url);
              ProtocolStatus status = output.getStatus();
              Content content = output.getContent();

              switch(status.getCode()) {

              case ProtocolStatus.SUCCESS:        // got a page
                output(key, datum, content, CrawlDatum.STATUS_FETCH_SUCCESS);
                updateStatus(content.getContent().length);
                break;

              case ProtocolStatus.MOVED:         // redirect
              case ProtocolStatus.TEMP_MOVED:
                String newUrl = status.getMessage();
                newUrl = URLFilters.filter(newUrl);
                if (newUrl != null && !newUrl.equals(url)) {
                  url = newUrl;
                  redirecting = true;
                  redirectCount++;
                  LOG.fine(" - redirect to " + url);
                } else {
                  LOG.fine(" - redirect skipped: " +
                           (url.equals(newUrl) ? "to same url" : "filtered"));
                }
                break;

              case ProtocolStatus.RETRY:          // retry
              case ProtocolStatus.EXCEPTION:
                output(key, datum, null, CrawlDatum.STATUS_FETCH_RETRY);
                break;
                
              case ProtocolStatus.GONE:           // gone
              case ProtocolStatus.NOTFOUND:
              case ProtocolStatus.ACCESS_DENIED:
              case ProtocolStatus.ROBOTS_DENIED:
              case ProtocolStatus.NOTMODIFIED:
                output(key, datum, null, CrawlDatum.STATUS_FETCH_GONE);
                break;

              default:
                LOG.warning("Unknown ProtocolStatus: " + status.getCode());
                output(key, datum, null, CrawlDatum.STATUS_FETCH_GONE);
              }

              if (redirecting && redirectCount >= maxRedirect) {
                LOG.info(" - redirect count exceeded " + url);
                output(key, datum, null, CrawlDatum.STATUS_FETCH_GONE);
              }

            } while (redirecting && (redirectCount < maxRedirect));

            
          } catch (Throwable t) {                 // unexpected exception
            logError(url, t);
            output(key, datum, null, CrawlDatum.STATUS_FETCH_GONE);
            
          }
        }

      } catch (Throwable e) {
        LOG.severe("fetcher caught:"+e.toString());
      } finally {
        synchronized (Fetcher.this) {activeThreads--;} // count threads
      }
    }

    private void logError(String url, Throwable t) {
      LOG.info("fetch of " + url + " failed with: " + t);
      LOG.log(Level.FINE, "stack", t);            // stack trace
      synchronized (Fetcher.this) {               // record failure
        errors++;
      }
    }

    private void output(UTF8 key, CrawlDatum datum,
                        Content content, int status) {

      datum.setStatus(status);

      if (content == null) {
        String url = key.toString();
        content = new Content(url, url, new byte[0], "", new Properties());
      }

      content.getMetadata().setProperty           // add digest to metadata
        (DIGEST_KEY, MD5Hash.digest(content.getContent()).toString());

      try {
        output.collect(key, new FetcherOutput(datum, content));
      } catch (IOException e) {
        LOG.severe("fetcher caught:"+e.toString());
      }
    }
    
  }

  public Fetcher() { super(null); }

  public Fetcher(NutchConf conf) { super(conf); }

  private synchronized void updateStatus(int bytesInPage) {
    pages++;
    bytes += bytesInPage;

    if ((pages % 100) == 0) {             // show status every 100pp
      long elapsed = (System.currentTimeMillis() - start)/1000;
      LOG.info( "status: "
                + pages + " pages, "
                + errors + " errors, "
                + bytes + " bytes, "
                + elapsed + " seconds");
      LOG.info("status: "
               + ((float)pages)/elapsed+" pages/s, "
               + ((((float)bytes)*8)/1024)/elapsed+" kb/s, "
               + ((float)bytes)/pages+" bytes/page");
    }
  }

  public void configure(JobConf job) {
    setConf(job);
    if (job.getBoolean("fetcher.verbose", false)) {
      LOG.setLevel(Level.FINE);
    }
  }

  public void run(RecordReader input, OutputCollector output)
    throws IOException {

    this.input = input;
    this.output = output;
			
    this.maxRedirect = getConf().getInt("http.redirect.max", 3);
    
    int threadCount = getConf().getInt("fetcher.threads.fetch", 10);
    for (int i = 0; i < threadCount; i++) {       // spawn threads
      new FetcherThread().start();
    }

    do {                                          // wait for threads to exit
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}

    } while (activeThreads > 0);
    
  }

  public void fetch(File segment, int threads)
    throws IOException {

    LOG.info("Fetcher: starting");
    LOG.info("Fetcher: segment: " + segment);
    LOG.info("Fetcher: threads: " + threads);


    JobConf job = new JobConf(getConf());

    job.setInt("fetcher.threads.fetch", threads);

    job.setInputDir(new File(segment, CrawlDatum.GENERATE_DIR_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(CrawlDatum.class);

    job.setMapRunnerClass(Fetcher.class);

    job.setOutputDir(segment);
    job.setOutputFormat(FetcherOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(FetcherOutput.class);

    JobClient.runJob(job);
    LOG.info("Fetcher: done");
  }


  /** Run the fetcher. */
  public static void main(String[] args) throws Exception {

    String usage = "Usage: Fetcher <segment> [-threads n]";

    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }
      
    File segment = new File(args[0]);

    NutchConf conf = NutchConf.get();

    int threads = conf.getInt("fetcher.threads.fetch", 10);

    for (int i = 2; i < args.length; i++) {       // parse command line
      if (args[i].equals("-threads")) {           // found -threads option
        threads =  Integer.parseInt(args[++i]);
      }
    }

    Fetcher fetcher = new Fetcher(conf);          // make a Fetcher
    
    fetcher.fetch(segment, threads);              // run the Fetcher

  }
}

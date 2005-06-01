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
import org.apache.nutch.util.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.mapred.*;

import java.util.logging.*;

/** The fetcher. Most of the work is done by plugins. */
public class Fetcher extends NutchConfigured implements MapRunnable { 

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.fetcher.Fetcher");
  
  private RecordReader input;
  private OutputCollector output;

  private int activeThreads;

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
            
            Protocol protocol = ProtocolFactory.getProtocol(url);
            Content content = protocol.getContent(url);

            output(url, datum, content, CrawlDatum.STATUS_FETCH_SUCCESS);
            
            updateStatus(content.getContent().length);

          } catch (ResourceGone e) {                // don't retry
            logError(url, e);
            output(url, datum, null, CrawlDatum.STATUS_FETCH_FAIL_PERM);
            
          } catch (Throwable t) {                   // retry all others
            logError(url, t);
            output(url, datum, null, CrawlDatum.STATUS_FETCH_FAIL_TEMP);

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

    private void output(String url, CrawlDatum datum,
                        Content content, int status) {
      datum.setStatus(status);
      if (content == null)
        content = new Content(url, url, new byte[0], "", new Properties());
      try {
        output.collect(new UTF8(url), new FetcherOutput(datum, content));
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

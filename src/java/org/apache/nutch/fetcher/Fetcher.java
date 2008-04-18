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

package org.apache.nutch.fetcher;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map.Entry;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.*;


/** The fetcher. Most of the work is done by plugins. */
public class Fetcher extends Configured implements Tool, MapRunnable<WritableComparable, Writable, Text, NutchWritable> { 

  public static final Log LOG = LogFactory.getLog(Fetcher.class);
  
  public static final int PERM_REFRESH_TIME = 5;

  public static final String CONTENT_REDIR = "content";

  public static final String PROTOCOL_REDIR = "protocol";

  public static class InputFormat extends SequenceFileInputFormat<WritableComparable, Writable> {
    /** Don't split inputs, to keep things polite. */
    public InputSplit[] getSplits(JobConf job, int nSplits)
      throws IOException {
      Path[] files = listPaths(job);
      FileSystem fs = FileSystem.get(job);
      InputSplit[] splits = new InputSplit[files.length];
      for (int i = 0; i < files.length; i++) {
        splits[i] = new FileSplit(files[i], 0, fs.getFileStatus(files[i]).getLen(), job);
      }
      return splits;
    }
  }

  private RecordReader<WritableComparable, Writable> input;
  private OutputCollector<Text, NutchWritable> output;
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
    private ScoringFilters scfilters;
    private ParseUtil parseUtil;
    private URLNormalizers normalizers;
    private ProtocolFactory protocolFactory;
    private boolean redirecting;
    private int redirectCount;
    private String reprUrl;

    public FetcherThread(Configuration conf) {
      this.setDaemon(true);                       // don't hang JVM on exit
      this.setName("FetcherThread");              // use an informative name
      this.conf = conf;
      this.urlFilters = new URLFilters(conf);
      this.scfilters = new ScoringFilters(conf);
      this.parseUtil = new ParseUtil(conf);
      this.protocolFactory = new ProtocolFactory(conf);
      this.normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_FETCHER);
    }

    public void run() {
      synchronized (Fetcher.this) {activeThreads++;} // count threads
      
      try {
        Text key = new Text();
        CrawlDatum datum = new CrawlDatum();
        
        while (true) {
          // TODO : NUTCH-258 ...
          // If something bad happened, then exit
          // if (conf.getBoolean("fetcher.exit", false)) {
          //   break;
          // ]
          
          try {                                   // get next entry from input
            if (!input.next(key, datum)) {
              break;                              // at eof, exit
            }
          } catch (IOException e) {
            if (LOG.isFatalEnabled()) {
              e.printStackTrace(LogUtil.getFatalStream(LOG));
              LOG.fatal("fetcher caught:"+e.toString());
            }
            break;
          }

          synchronized (Fetcher.this) {
            lastRequestStart = System.currentTimeMillis();
          }

          // url may be changed through redirects.
          Text url = new Text(key);

          Text reprUrlWritable =
            (Text) datum.getMetaData().get(Nutch.WRITABLE_REPR_URL_KEY);
          if (reprUrlWritable == null) {
            reprUrl = key.toString();
          } else {
            reprUrl = reprUrlWritable.toString();
          }

          try {
            if (LOG.isInfoEnabled()) { LOG.info("fetching " + url); }

            // fetch the page
            redirectCount = 0;
            do {
              if (LOG.isDebugEnabled()) {
                LOG.debug("redirectCount=" + redirectCount);
              }
              redirecting = false;
              Protocol protocol = this.protocolFactory.getProtocol(url.toString());
              ProtocolOutput output = protocol.getProtocolOutput(url, datum);
              ProtocolStatus status = output.getStatus();
              Content content = output.getContent();
              ParseStatus pstatus = null;

              String urlString = url.toString();
              if (reprUrl != null && !reprUrl.equals(urlString)) {
                datum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY,
                    new Text(reprUrl));
              }

              switch(status.getCode()) {

              case ProtocolStatus.SUCCESS:        // got a page
                pstatus = output(url, datum, content, status, CrawlDatum.STATUS_FETCH_SUCCESS);
                updateStatus(content.getContent().length);
                if (pstatus != null && pstatus.isSuccess() &&
                        pstatus.getMinorCode() == ParseStatus.SUCCESS_REDIRECT) {
                  String newUrl = pstatus.getMessage();
                  int refreshTime = Integer.valueOf(pstatus.getArgs()[1]);
                  url = handleRedirect(url, datum, urlString, newUrl,
                                       refreshTime < PERM_REFRESH_TIME,
                                       CONTENT_REDIR);
                }
                break;

              case ProtocolStatus.MOVED:         // redirect
              case ProtocolStatus.TEMP_MOVED:
                int code;
                boolean temp;
                if (status.getCode() == ProtocolStatus.MOVED) {
                  code = CrawlDatum.STATUS_FETCH_REDIR_PERM;
                  temp = false;
                } else {
                  code = CrawlDatum.STATUS_FETCH_REDIR_TEMP;
                  temp = true;
                }
                output(url, datum, content, status, code);
                String newUrl = status.getMessage();
                url = handleRedirect(url, datum, urlString, newUrl,
                                     temp, PROTOCOL_REDIR);
                break;

              // failures - increase the retry counter
              case ProtocolStatus.EXCEPTION:
                logError(url, status.getMessage());
              /* FALLTHROUGH */
              case ProtocolStatus.RETRY:          // retry
              case ProtocolStatus.WOULDBLOCK:
              case ProtocolStatus.BLOCKED:
                output(url, datum, null, status, CrawlDatum.STATUS_FETCH_RETRY);
                break;
                
              // permanent failures
              case ProtocolStatus.GONE:           // gone
              case ProtocolStatus.NOTFOUND:
              case ProtocolStatus.ACCESS_DENIED:
              case ProtocolStatus.ROBOTS_DENIED:
                output(url, datum, null, status, CrawlDatum.STATUS_FETCH_GONE);
                break;

              case ProtocolStatus.NOTMODIFIED:
                output(url, datum, null, status, CrawlDatum.STATUS_FETCH_NOTMODIFIED);
                break;
                
              default:
                if (LOG.isWarnEnabled()) {
                  LOG.warn("Unknown ProtocolStatus: " + status.getCode());
                }
                output(url, datum, null, status, CrawlDatum.STATUS_FETCH_GONE);
              }

              if (redirecting && redirectCount >= maxRedirect) {
                if (LOG.isInfoEnabled()) {
                  LOG.info(" - redirect count exceeded " + url);
                }
                output(url, datum, null, status, CrawlDatum.STATUS_FETCH_GONE);
              }

            } while (redirecting && (redirectCount < maxRedirect));

            
          } catch (Throwable t) {                 // unexpected exception
            logError(url, t.toString());
            output(url, datum, null, null, CrawlDatum.STATUS_FETCH_RETRY);
            
          }
        }

      } catch (Throwable e) {
        if (LOG.isFatalEnabled()) {
          e.printStackTrace(LogUtil.getFatalStream(LOG));
          LOG.fatal("fetcher caught:"+e.toString());
        }
      } finally {
        synchronized (Fetcher.this) {activeThreads--;} // count threads
      }
    }

    private Text handleRedirect(Text url, CrawlDatum datum,
                                String urlString, String newUrl,
                                boolean temp, String redirType)
    throws MalformedURLException, URLFilterException {
      newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
      newUrl = urlFilters.filter(newUrl);
      if (newUrl != null && !newUrl.equals(urlString)) {
        reprUrl = URLUtil.chooseRepr(reprUrl, newUrl, temp);
        url = new Text(newUrl);
        if (maxRedirect > 0) {
          redirecting = true;
          redirectCount++;
          if (LOG.isDebugEnabled()) {
            LOG.debug(" - " + redirType + " redirect to " +
                      url + " (fetching now)");
          }
          return url;
        } else {
          CrawlDatum newDatum = new CrawlDatum();
          if (reprUrl != null) {
            newDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY,
                new Text(reprUrl));
          }
          output(url, newDatum, null, null, CrawlDatum.STATUS_LINKED);
          if (LOG.isDebugEnabled()) {
            LOG.debug(" - " + redirType + " redirect to " +
                      url + " (fetching later)");
          }
          return null;
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug(" - " + redirType + " redirect skipped: " +
              (newUrl != null ? "to same url" : "filtered"));
        }
        return null;
      }
    }

    private void logError(Text url, String message) {
      if (LOG.isInfoEnabled()) {
        LOG.info("fetch of " + url + " failed with: " + message);
      }
      synchronized (Fetcher.this) {               // record failure
        errors++;
      }
    }

    private ParseStatus output(Text key, CrawlDatum datum,
                        Content content, ProtocolStatus pstatus, int status) {

      datum.setStatus(status);
      datum.setFetchTime(System.currentTimeMillis());
      if (pstatus != null) datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY, pstatus);

      ParseResult parseResult = null;
      if (content != null) {
        Metadata metadata = content.getMetadata();
        // add segment to metadata
        metadata.set(Nutch.SEGMENT_NAME_KEY, segmentName);
        // add score to content metadata so that ParseSegment can pick it up.
        try {
          scfilters.passScoreBeforeParsing(key, datum, content);
        } catch (Exception e) {
          if (LOG.isWarnEnabled()) {
            e.printStackTrace(LogUtil.getWarnStream(LOG));
            LOG.warn("Couldn't pass score, url " + key + " (" + e + ")");
          }
        }
        /* Note: Fetcher will only follow meta-redirects coming from the
         * original URL. */ 
        if (parsing && status == CrawlDatum.STATUS_FETCH_SUCCESS) {
          try {
            parseResult = this.parseUtil.parse(content);
          } catch (Exception e) {
            LOG.warn("Error parsing: " + key + ": " + StringUtils.stringifyException(e));
          }

          if (parseResult == null) {
            byte[] signature = 
              SignatureFactory.getSignature(getConf()).calculate(content, 
                  new ParseStatus().getEmptyParse(conf));
            datum.setSignature(signature);
          }
        }
        
        if (!parsing) {
          /* Store status code in content if we are not parsing in fetcher. 
           * So we can read this value during parsing (as a separate job) and
           * decide to parse or not.
           * */
          content.getMetadata().add(Nutch.FETCH_STATUS_KEY, Integer.toString(status));
        }
      }

      try {
        output.collect(key, new NutchWritable(datum));
        if (content != null && storingContent)
          output.collect(key, new NutchWritable(content));
        if (parseResult != null) {
          for (Entry<Text, Parse> entry : parseResult) {
            Text url = entry.getKey();
            Parse parse = entry.getValue();
            ParseStatus parseStatus = parse.getData().getStatus();
            
            if (!parseStatus.isSuccess()) {
              LOG.warn("Error parsing: " + key + ": " + parseStatus);
              parse = parseStatus.getEmptyParse(getConf());
            }

            // Calculate page signature. For non-parsing fetchers this will
            // be done in ParseSegment
            byte[] signature = 
              SignatureFactory.getSignature(getConf()).calculate(content, parse);
            // Ensure segment name and score are in parseData metadata
            parse.getData().getContentMeta().set(Nutch.SEGMENT_NAME_KEY, 
                segmentName);
            parse.getData().getContentMeta().set(Nutch.SIGNATURE_KEY, 
                StringUtil.toHexString(signature));
            // Pass fetch time to content meta
            parse.getData().getContentMeta().set(Nutch.FETCH_TIME_KEY,
                Long.toString(datum.getFetchTime()));
            if (url.equals(key))
              datum.setSignature(signature);
            try {
              scfilters.passScoreAfterParsing(url, content, parse);
            } catch (Exception e) {
              if (LOG.isWarnEnabled()) {
                e.printStackTrace(LogUtil.getWarnStream(LOG));
                LOG.warn("Couldn't pass score, url " + key + " (" + e + ")");
              }
            }
            output.collect(url, new NutchWritable(
                    new ParseImpl(new ParseText(parse.getText()), 
                                  parse.getData(), parse.isCanonical())));
          }
        }
      } catch (IOException e) {
        if (LOG.isFatalEnabled()) {
          e.printStackTrace(LogUtil.getFatalStream(LOG));
          LOG.fatal("fetcher caught:"+e.toString());
        }
      }

      // return parse status if it exits
      if (parseResult != null && !parseResult.isEmpty()) {
        Parse p = parseResult.get(content.getUrl());
        if (p != null) {
          return p.getData().getStatus();
        }
      } 
      return null;
    }
    
  }

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

  public Fetcher() {
    
  }
  
  public Fetcher(Configuration conf) {
    setConf(conf);
  }
  
  public void configure(JobConf job) {
    setConf(job);

    this.segmentName = job.get(Nutch.SEGMENT_NAME_KEY);
    this.storingContent = isStoringContent(job);
    this.parsing = isParsing(job);

//    if (job.getBoolean("fetcher.verbose", false)) {
//      LOG.setLevel(Level.FINE);
//    }
  }

  public void close() {}

  public static boolean isParsing(Configuration conf) {
    return conf.getBoolean("fetcher.parse", true);
  }

  public static boolean isStoringContent(Configuration conf) {
    return conf.getBoolean("fetcher.store.content", true);
  }

  public void run(RecordReader<WritableComparable, Writable> input, OutputCollector<Text, NutchWritable> output,
                  Reporter reporter) throws IOException {

    this.input = input;
    this.output = output;
    this.reporter = reporter;

    this.maxRedirect = getConf().getInt("http.redirect.max", 3);
    
    int threadCount = getConf().getInt("fetcher.threads.fetch", 10);
    if (LOG.isInfoEnabled()) { LOG.info("Fetcher: threads: " + threadCount); }

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
          if (LOG.isWarnEnabled()) {
            LOG.warn("Aborting with "+activeThreads+" hung threads.");
          }
          return;
        }
      }

    } while (activeThreads > 0);
    
  }

  public void fetch(Path segment, int threads)
    throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("Fetcher: starting");
      LOG.info("Fetcher: segment: " + segment);
    }

    JobConf job = new NutchJob(getConf());
    job.setJobName("fetch " + segment);

    job.setInt("fetcher.threads.fetch", threads);
    job.set(Nutch.SEGMENT_NAME_KEY, segment.getName());

    // for politeness, don't permit parallel execution of a single task
    job.setSpeculativeExecution(false);

    job.setInputPath(new Path(segment, CrawlDatum.GENERATE_DIR_NAME));
    job.setInputFormat(InputFormat.class);

    job.setMapRunnerClass(Fetcher.class);

    job.setOutputPath(segment);
    job.setOutputFormat(FetcherOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NutchWritable.class);

    JobClient.runJob(job);
    if (LOG.isInfoEnabled()) { LOG.info("Fetcher: done"); }
  }


  /** Run the fetcher. */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new Fetcher(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {

    String usage = "Usage: Fetcher <segment> [-threads n] [-noParsing]";

    if (args.length < 1) {
      System.err.println(usage);
      return -1;
    }
      
    Path segment = new Path(args[0]);
    int threads = getConf().getInt("fetcher.threads.fetch", 10);
    boolean parsing = true;

    for (int i = 1; i < args.length; i++) {       // parse command line
      if (args[i].equals("-threads")) {           // found -threads option
        threads =  Integer.parseInt(args[++i]);
      } else if (args[i].equals("-noParsing")) parsing = false;
    }

    getConf().setInt("fetcher.threads.fetch", threads);
    if (!parsing) {
      getConf().setBoolean("fetcher.parse", parsing);
    }
    try {
      fetch(segment, threads);              // run the Fetcher
      return 0;
    } catch (Exception e) {
      LOG.fatal("Fetcher: " + StringUtils.stringifyException(e));
      return -1;
    }

  }
}

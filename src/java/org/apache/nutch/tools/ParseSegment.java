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

package org.apache.nutch.tools;

import org.apache.nutch.pagedb.FetchListEntry;
import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.plugin.*;

import org.apache.nutch.fetcher.FetcherOutput;

import java.io.EOFException;
import java.io.File;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Properties;
import java.util.logging.*;

/**
 * Parse contents in one segment.
 *
 * <p>
 * It assumes, under given segment, existence of ./fetcher_output/,
 * which is typically generated after a non-parsing fetcher run
 * (i.e., fetcher is started with option -noParsing).
 *
 * <p> Contents in one segemnt are parsed and saved in these steps:
 * <li> (1) ./fetcher_output/ and ./content/ are looped together
 * (possibly by multiple ParserThreads), and content is parsed for each entry.
 * The entry number and resultant ParserOutput are saved in ./parser.unsorted.
 * <li> (2) ./parser.unsorted is sorted by entry number, result saved as
 * ./parser.sorted.
 * <li> (3) ./parser.sorted and ./fetcher_output/ are looped together.
 * At each entry, ParserOutput is split into ParseDate and ParseText,
 * which are saved in ./parse_data/ and ./parse_text/ respectively. Also
 * updated is FetcherOutput with parsing status, which is saved in ./fetcher/.
 *
 * <p> In the end, ./fetcher/ should be identical to one resulted from
 * fetcher run WITHOUT option -noParsing.
 *
 * <p> By default, intermediates ./parser.unsorted and ./parser.sorted
 * are removed at the end, unless option -noClean is used. However
 * ./fetcher_output/ is kept intact.
 *
 * <p> Check Fetcher.java and FetcherOutput.java for further discussion.
 *
 * @author John Xing
 */

public class ParseSegment {

  public static final Logger LOG =
    LogFormatter.getLogger(ParseSegment.class.getName());

  private int threadCount =                       // max number of threads
    NutchConf.get().getInt("parser.threads.parse", 10);

  private NutchFileSystem nfs;

  // segment dir
  private String directory;

  // readers for FetcherOutput (no-parsing) and Content
  private ArrayFile.Reader fetcherNPReader;
  private ArrayFile.Reader contentReader;

  // SequenceFile (unsorted) for ParserOutput
  private File unsortedFile;
  private SequenceFile.Writer parserOutputWriter;

  // SequenceFile (sorted) for ParserOutput
  private File sortedFile;

  // whether dryRun only (i.e., no real parsing is done)
  private boolean dryRun = false;

  // whether clean intermediate files
  private boolean clean = true;

  // entry (record number) in fetcherNPReader (same in contentReader)
  private long entry = -1;

  // for stats
  private long start;                             // start time
  private long bytes;                             // total bytes parsed
  private int pages;                              // total pages parsed
  private int errors;                             // total pages errored

  private ThreadGroup group = new ThreadGroup("parser"); // our thread group

  /**
   * Inner class ParserThread
   */
  private class ParserThread extends Thread {

    // current entry that this thread is parsing
    private long myEntry = -1;

    // for detailed stats
    private long t0,t1,t2,t3,t4,t5;

    public ParserThread() { super(group, "myThread"); }

    /**
     * This thread participates in looping through
     * entries of FetcherOutput and Content
     */
    public void run() {

      FetcherOutput fetcherOutput = new FetcherOutput();
      Content content = new Content();

      FetchListEntry fle = null;
      String url = null;

      while (true) {
        if (LogFormatter.hasLoggedSevere())       // something bad happened
          break;                                  // exit

        t0 = System.currentTimeMillis();

        try {

          // must be read in order! thus synchronize threads.
          synchronized (ParseSegment.this) {
            t1 = System.currentTimeMillis();

            try {
              if (fetcherNPReader.next(fetcherOutput) == null ||
                contentReader.next(content) == null)
              return;
            } catch (EOFException eof) {
              // only partial data available, stop this thread,
              // other threads will be stopped also.
              return;
            }

            entry++;
            myEntry = entry;
            if (LOG.isLoggable(Level.FINE))
              LOG.fine("Read in entry "+entry);

            // safe guard against mismatched files
            //if (entry != fetcherNPReader.key() ||
            //    entry != contentReader.key()) {
            //  LOG.severe("Mismatched entries under "
            //    + FetcherOutput.DIR_NAME_NP + " and " + Content.DIR_NAME);
            //  continue;
            //}
          }

          t2 = System.currentTimeMillis();

          fle = fetcherOutput.getFetchListEntry();
          url = fle.getPage().getURL().toString();

          LOG.fine("parsing " + url);            // parse the page

          // safe guard against mismatched files
          if (!url.equals(content.getUrl())) {
            LOG.severe("Mismatched entries under "
              + FetcherOutput.DIR_NAME_NP + " (" + url +
              ") and " + Content.DIR_NAME + " (" + content.getUrl() + ")");
            continue;
          }

          // if fetch was successful or
          // previously unable to parse (so try again)
          ProtocolStatus ps = fetcherOutput.getProtocolStatus();
          if (ps.isSuccess()) {
            handleContent(url, content);
            synchronized (ParseSegment.this) {
              pages++;                    // record successful parse
              bytes += content.getContent().length;
              if ((pages % 100) == 0)
                status();
            }
          } else {
            // errored at fetch step
            logError(url, new ProtocolException("Error at fetch stage: " + ps));
            handleNoContent(new ParseStatus(ParseStatus.FAILED_MISSING_CONTENT));
          }

        } catch (ParseException e) {
          logError(url, e);
          handleNoContent(new ParseStatus(e));

        } catch (Throwable t) {                   // an unchecked exception
          if (fle != null) {
            logError(url, t);
            handleNoContent(new ParseStatus(t));
          } else {
            LOG.severe("Unexpected exception");
          }
        }
      }
    }

    private void logError(String url, Throwable t) {
      LOG.info("parse of " + url + " failed with: " + t);
      if (LOG.isLoggable(Level.FINE))
        LOG.log(Level.FINE, "stack", t);               // stack trace
      synchronized (ParseSegment.this) {               // record failure
        errors++;
      }
    }

    private void handleContent(String url, Content content)
      throws ParseException {

      //String contentType = content.getContentType();
      String contentType = content.getMetadata().getProperty("Content-Type");

      if (ParseSegment.this.dryRun) {
        LOG.info("To be handled as Content-Type: "+contentType);
        return;
      }

      Parser parser = ParserFactory.getParser(contentType, url);
      Parse parse = parser.getParse(content);

      outputPage
        (new ParseText(parse.getText()), parse.getData());
    }

    private void handleNoContent(ParseStatus status) {
      if (ParseSegment.this.dryRun) {
        LOG.info("To be handled as no content");
        return;
      }
      outputPage(new ParseText(""),
                 new ParseData(status, "", new Outlink[0], new Properties()));
    }
      
    private void outputPage
      (ParseText parseText, ParseData parseData) {
      try {
        t3 = System.currentTimeMillis();
        synchronized (parserOutputWriter) {
          t4 = System.currentTimeMillis();
          parserOutputWriter.append(new LongWritable(myEntry),
            new ParserOutput(parseData, parseText));
          t5 = System.currentTimeMillis();
          if (LOG.isLoggable(Level.FINE))
            LOG.fine("Entry: "+myEntry
              +" "+parseData.getMetadata().getProperty("Content-Length")
              +" wait="+(t1-t0) +" read="+(t2-t1) +" parse="+(t3-t2)
              +" wait="+(t4-t3) +" write="+(t5-t4) +"ms");
        }
      } catch (Throwable t) {
        LOG.severe("error writing output:" + t.toString());
      }
    }

  }

  /**
   * Inner class ParserOutput: ParseData + ParseText
   */
  private class ParserOutput extends VersionedWritable {
    public static final String DIR_NAME = "parser";

    private final static byte VERSION = 2;

    private ParseData parseData = new ParseData();
    private ParseText parseText = new ParseText();

    public ParserOutput() {}
    
    public ParserOutput(ParseData parseData, ParseText parseText) {
      this.parseData = parseData;
      this.parseText = parseText;
    }

    public byte getVersion() { return VERSION; }

    public ParseData getParseData() {
      return this.parseData;
    }

    public ParseText getParseText() {
      return this.parseText;
    }

    public final void readFields(DataInput in) throws IOException {
      super.readFields(in);                         // check version
      parseData.readFields(in);
      parseText.readFields(in);
      return;
    }

    public final void write(DataOutput out) throws IOException {
      super.write(out);                             // write version
      parseData.write(out);
      parseText.write(out);
      return;
    }
  }
			
  /**
   * ParseSegment constructor
   */
  public ParseSegment(NutchFileSystem nfs, String directory, boolean dryRun)
    throws IOException {

    File file;

    this.nfs = nfs;
    this.directory = directory;
    this.dryRun = dryRun;

    // FetcherOutput.DIR_NAME_NP must exist
    file = new File(directory, FetcherOutput.DIR_NAME_NP);
    if (!nfs.exists(file))
      throw new IOException("Directory missing: "+FetcherOutput.DIR_NAME_NP);

    if (dryRun)
      return;

    // clean old FetcherOutput.DIR_NAME
    file = new File(directory, FetcherOutput.DIR_NAME);
    if (nfs.exists(file)) {
      LOG.info("Deleting old "+file.getName());
      nfs.delete(file);
    }

    // clean old unsortedFile
    this.unsortedFile = new File(directory, ParserOutput.DIR_NAME+".unsorted");
    if (nfs.exists(this.unsortedFile)) {
      LOG.info("Deleting old "+this.unsortedFile.getName());
      nfs.delete(this.unsortedFile);
    }

    // clean old sortedFile
    this.sortedFile = new File(directory, ParserOutput.DIR_NAME+".sorted");
    if (nfs.exists(this.sortedFile)) {
      LOG.info("Deleting old "+this.sortedFile.getName());
      nfs.delete(this.sortedFile);
    }

    // clean old ParseData.DIR_NAME
    file = new File(directory, ParseData.DIR_NAME);
    if (nfs.exists(file)) {
      LOG.info("Deleting old "+file.getName());
      nfs.delete(file);
    }

    // clean old ParseText.DIR_NAME
    file = new File(directory, ParseText.DIR_NAME);
    if (nfs.exists(file)) {
      LOG.info("Deleting old "+file.getName());
      nfs.delete(file);
    }

  }

  /** Set thread count */
  public void setThreadCount(int threadCount) {
    this.threadCount=threadCount;
  }

  /** Set the logging level. */
  public static void setLogLevel(Level level) {
    LOG.setLevel(level);
    PluginRepository.LOG.setLevel(level);
    ParserFactory.LOG.setLevel(level);
    LOG.info("logging at " + level);
  }

  /** Set if clean intermediates. */
  public void setClean(boolean clean) {
    this.clean = clean;
  }

  /** Display the status of the parser run. */
  public void status() {
    long ms = System.currentTimeMillis() - start;
    LOG.info("status: "
             + pages + " pages, "
             + errors + " errors, "
             + bytes + " bytes, "
             + ms + " ms");
    LOG.info("status: "
             + (((float)pages)/(ms/1000.0f))+" pages/s, "
             + (((float)bytes*8/1024)/(ms/1000.0f))+" kb/s, "
             + (((float)bytes)/pages) + " bytes/page");
  }

  /** Parse contents by multiple threads and save as unsorted ParserOutput */
  public void parse() throws IOException, InterruptedException {

    fetcherNPReader = new ArrayFile.Reader
      (nfs, (new File(directory, FetcherOutput.DIR_NAME_NP)).getPath());
    contentReader = new ArrayFile.Reader
      (nfs, (new File(directory, Content.DIR_NAME)).getPath());

    if (!this.dryRun) {
      parserOutputWriter = new SequenceFile.Writer
        (nfs, unsortedFile.getPath(), LongWritable.class, ParserOutput.class);
    }

    start = System.currentTimeMillis();

    for (int i = 0; i < threadCount; i++) {       // spawn threads
      ParserThread thread = new ParserThread(); 
      thread.start();
    }

    do {
      Thread.sleep(1000);

      if (LogFormatter.hasLoggedSevere()) 
        throw new RuntimeException("SEVERE error logged.  Exiting parser.");

    } while (group.activeCount() > 0);            // wait for threads to finish

    fetcherNPReader.close();
    contentReader.close();
    if (!this.dryRun)
      parserOutputWriter.close();

    status();                                     // print final status
  }

  /** Sort ParserOutput */
  public void sort() throws IOException {

    if (this.dryRun)
      return;

    LOG.info("Sorting ParserOutput");

    start = System.currentTimeMillis();

    SequenceFile.Sorter sorter = new SequenceFile.Sorter
      (nfs, new LongWritable.Comparator(), ParserOutput.class);

    sorter.sort(unsortedFile.getPath(), sortedFile.getPath());

    double localSecs = (System.currentTimeMillis() - start) / 1000.0;
    LOG.info("Sorted: " + (pages+errors) + " entries in " + localSecs + "s, "
      + ((pages+errors)/localSecs) + " entries/s");

    if (this.clean) {
      LOG.info("Deleting intermediate "+unsortedFile.getName());
      nfs.delete(unsortedFile);
    }

    return;
  }

  /**
   * Split sorted ParserOutput into ParseData and ParseText,
   * and generate new FetcherOutput with updated status
   */
  public void save() throws IOException {

    if (this.dryRun)
      return;

    LOG.info("Saving ParseData and ParseText separately");

    start = System.currentTimeMillis();

    SequenceFile.Reader parserOutputReader
      = new SequenceFile.Reader(nfs, sortedFile.getPath());

    ArrayFile.Reader fetcherNPReader = new ArrayFile.Reader(nfs,
      (new File(directory, FetcherOutput.DIR_NAME_NP)).getPath());

    ArrayFile.Writer fetcherWriter = new ArrayFile.Writer(nfs,
      (new File(directory, FetcherOutput.DIR_NAME)).getPath(),
      FetcherOutput.class);

    ArrayFile.Writer parseDataWriter = new ArrayFile.Writer(nfs,
      (new File(directory, ParseData.DIR_NAME)).getPath(), ParseData.class);
    ArrayFile.Writer parseTextWriter = new ArrayFile.Writer(nfs,
      (new File(directory, ParseText.DIR_NAME)).getPath(), ParseText.class);

    try {
      LongWritable key = new LongWritable();
      ParserOutput val = new ParserOutput();
      FetcherOutput fo = new FetcherOutput();
      int count = 0;
      int status;
      while (parserOutputReader.next(key,val)) {
        fetcherNPReader.next(fo);
        // safe guarding
        if (fetcherNPReader.key() != key.get())
          throw new IOException("Mismatch between entries under "
            + FetcherOutput.DIR_NAME_NP + " and in " + sortedFile.getName());
        fetcherWriter.append(fo);
        parseDataWriter.append(val.getParseData());
        parseTextWriter.append(val.getParseText());
        count++;
      }
      // safe guard! make sure there are identical entries
      // in (fetcher, content) and in (parseData, parseText)
      if (count != (pages+errors))
        throw new IOException("Missing entries: expect "+(pages+errors)
          +", but have "+count+" entries instead.");
    } finally {
      fetcherNPReader.close();
      fetcherWriter.close();
      parseDataWriter.close();
      parseTextWriter.close();
      parserOutputReader.close();
    }

    double localSecs = (System.currentTimeMillis() - start) / 1000.0;
    LOG.info("Saved: " + (pages+errors) + " entries in " + localSecs + "s, "
      + ((pages+errors)/localSecs) + " entries/s");

    if (this.clean) {
      LOG.info("Deleting intermediate "+sortedFile.getName());
      nfs.delete(sortedFile);
    }

    return;
  }

  /** main method */
  public static void main(String[] args) throws Exception {
    int threadCount = -1;
    boolean showThreadID = false;
    boolean dryRun = false;
    String logLevel = "info";
    boolean clean = true;
    String directory = null;

    String usage = "Usage: ParseSegment (-local | -ndfs <namenode:port>) [-threads n] [-showThreadID] [-dryRun] [-logLevel level] [-noClean] dir";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }
      
    // parse command line
    NutchFileSystem nfs = NutchFileSystem.parseArgs(args, 0);

    for (int i = 0; i < args.length; i++) {
      if (args[i] == null) {
          continue;
      } else if (args[i].equals("-threads")) {
        threadCount =  Integer.parseInt(args[++i]);
      } else if (args[i].equals("-showThreadID")) {
        showThreadID = true;
      } else if (args[i].equals("-dryRun")) {
        dryRun = true;
      } else if (args[i].equals("-logLevel")) {
        logLevel = args[++i];
      } else if (args[i].equals("-noClean")) {
        clean = false;
      } else {
        directory = args[i];
      }
    }

    try {

      ParseSegment parseSegment = new ParseSegment(nfs, directory, dryRun);

      parseSegment.setLogLevel
        (Level.parse((new String(logLevel)).toUpperCase()));

      if (threadCount != -1)
        parseSegment.setThreadCount(threadCount);
      if (showThreadID)
        LogFormatter.setShowThreadIDs(showThreadID);

      parseSegment.setClean(clean);

      parseSegment.parse();
      parseSegment.sort();
      parseSegment.save();

    } finally {
      nfs.close();
    }

  }
}

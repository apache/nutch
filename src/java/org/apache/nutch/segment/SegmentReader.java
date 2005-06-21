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

package org.apache.nutch.segment;

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;
import java.util.logging.Logger;

import org.apache.nutch.fetcher.FetcherOutput;
import org.apache.nutch.io.ArrayFile;
import org.apache.nutch.io.LongWritable;
import org.apache.nutch.io.MapFile;
import org.apache.nutch.io.SequenceFile;
import org.apache.nutch.io.UTF8;
import org.apache.nutch.fs.*;
import org.apache.nutch.pagedb.FetchListEntry;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.LogFormatter;

/**
 * This class holds together all data readers for an existing segment.
 * Some convenience methods are also provided, to read from the segment and
 * to reposition the current pointer.
 * 
 * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
 */
public class SegmentReader {
  public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.segment.SegmentReader");
  
  public ArrayFile.Reader fetcherReader;
  public ArrayFile.Reader contentReader;
  public ArrayFile.Reader parseTextReader;
  public ArrayFile.Reader parseDataReader;
  public boolean isParsed = false;

  /**
   * The time when fetching of this segment started, as recorded
   * in fetcher output data.
   */
  public long started = 0L;
  /**
   * The time when fetching of this segment finished, as recorded
   * in fetcher output data.
   */
  public long finished = 0L;
  public long size = 0L;
  private long key = -1L;

  
  public File segmentDir;
  public NutchFileSystem nfs;

  /**
   * Open a segment for reading. If the segment is corrupted, do not attempt to fix it.
   * @param dir directory containing segment data
   * @throws Exception
   */
  public SegmentReader(File dir) throws Exception {
    this(new LocalFileSystem(), dir, true, true, true, false);
  }
  
  /**
   * Open a segment for reading. If segment is corrupted, do not attempt to fix it.
   * @param nfs filesystem
   * @param dir directory containing segment data
   * @throws Exception
   */
  public SegmentReader(NutchFileSystem nfs, File dir) throws Exception {
    this(nfs, dir, true, true, true, false);
  }
  
  /**
   * Open a segment for reading.
   * @param dir directory containing segment data
   * @param autoFix if true, and the segment is corrupted, attempt to 
   * fix errors and try to open it again. If the segment is corrupted, and
   * autoFix is false, or it was not possible to correct errors, an Exception is
   * thrown.
   * @throws Exception
   */
  public SegmentReader(File dir, boolean autoFix) throws Exception {
    this(new LocalFileSystem(), dir, true, true, true, autoFix);
  }
  
  /**
   * Open a segment for reading.
   * @param nfs filesystem
   * @param dir directory containing segment data
   * @param autoFix if true, and the segment is corrupted, attempt to 
   * fix errors and try to open it again. If the segment is corrupted, and
   * autoFix is false, or it was not possible to correct errors, an Exception is
   * thrown.
   * @throws Exception
   */
  public SegmentReader(NutchFileSystem nfs, File dir, boolean autoFix) throws Exception {
    this(nfs, dir, true, true, true, autoFix);
  }
  
  /**
   * Open a segment for reading. When a segment is open, its total size is checked
   * and cached in this class - however, only by actually reading entries one can
   * be sure about the exact number of valid, non-corrupt entries.
   * 
   * <p>If the segment was created with no-parse option (see {@link FetcherOutput#DIR_NAME_NP})
   * then automatically withParseText and withParseData will be forced to false.</p>
   * 
   * @param nfs NutchFileSystem to use
   * @param dir directory containing segment data
   * @param withContent if true, read Content, otherwise ignore it
   * @param withParseText if true, read ParseText, otherwise ignore it
   * @param withParseData if true, read ParseData, otherwise ignore it
   * @param autoFix if true, and the segment is corrupt, try to automatically fix it.
   * If this parameter is false, and the segment is corrupt, or fixing was unsuccessful,
   * and Exception is thrown.
   * @throws Exception
   */
  public SegmentReader(NutchFileSystem nfs, File dir,
          boolean withContent, boolean withParseText, boolean withParseData,
          boolean autoFix) throws Exception {
    isParsed = isParsedSegment(nfs, dir);
    if (!isParsed) {
      withParseText = false;
      withParseData = false;
    }
    try {
      init(nfs, dir, withContent, withParseText, withParseData);
    } catch (Exception e) {
      boolean ok = false;
      if (autoFix) {
        // corrupt segment, attempt to fix
        ok = fixSegment(nfs, dir, withContent, withParseText, withParseData, false);
      }
      if (ok)
        init(nfs, dir, withContent, withParseText, withParseData);
      else throw new Exception("Segment " + dir + " is corrupted.");
    }
  }

  public static boolean isParsedSegment(NutchFileSystem nfs, File segdir) throws Exception {
    boolean res;
    File foDir = new File(segdir, FetcherOutput.DIR_NAME);
    if (nfs.exists(foDir) && nfs.isDirectory(foDir)) return true;
    foDir = new File(segdir, FetcherOutput.DIR_NAME_NP);
    if (nfs.exists(foDir) && nfs.isDirectory(foDir)) return false;
    throw new Exception("Missing or invalid '" + FetcherOutput.DIR_NAME + "' or '"
            + FetcherOutput.DIR_NAME_NP + "' directory in " + segdir);
  }
  
  /**
   * Attempt to fix a partially corrupted segment. Currently this means just
   * fixing broken MapFile's, using {@link MapFile#fix(NutchFileSystem, File, Class, Class, boolean)}
   * method.
   * @param nfs filesystem
   * @param dir segment directory
   * @param withContent if true, fix content, otherwise ignore it
   * @param withParseText if true, fix parse_text, otherwise ignore it
   * @param withParseData if true, fix parse_data, otherwise ignore it
   * @param dryrun if true, only show what would be done without performing any actions
   * @return
   */
  public static boolean fixSegment(NutchFileSystem nfs, File dir, 
          boolean withContent, boolean withParseText, boolean withParseData,
          boolean dryrun) {
    String dr = "";
    if (dryrun) dr = "[DRY RUN] ";
    File fetcherOutput = null;
    File content = new File(dir, Content.DIR_NAME);
    File parseData = new File(dir, ParseData.DIR_NAME);
    File parseText = new File(dir, ParseText.DIR_NAME);
    long cnt = 0L;
    try {
      if (isParsedSegment(nfs, dir)) {
        fetcherOutput = new File(dir, FetcherOutput.DIR_NAME);
      } else {
        fetcherOutput = new File(dir, FetcherOutput.DIR_NAME_NP);
        withParseText = false;
        withParseData = false;
      }
      cnt = MapFile.fix(nfs, fetcherOutput, LongWritable.class, FetcherOutput.class, dryrun);
      if (cnt != -1) LOG.info(dr + " - fixed " + fetcherOutput.getName());
      if (withContent) {
        cnt = MapFile.fix(nfs, content, LongWritable.class, Content.class, dryrun);
        if (cnt != -1) LOG.info(dr + " - fixed " + content.getName());
      }
      if (withParseData) {
        cnt = MapFile.fix(nfs, parseData, LongWritable.class, ParseData.class, dryrun);
        if (cnt != -1) LOG.info(dr + " - fixed " + parseData.getName());
      }
      if (withParseText) {
        cnt = MapFile.fix(nfs, parseText, LongWritable.class, ParseText.class, dryrun);
        if (cnt != -1) LOG.info(dr + " - fixed " + parseText.getName());
      }
      LOG.info(dr + "Finished fixing " + dir.getName());
      return true;
    } catch (Throwable t) {
      LOG.warning(dr + "Unable to fix segment " + dir.getName() + ": " + t.getMessage());
      return false;
    }
  }

  private void init(NutchFileSystem nfs, File dir,
          boolean withContent, boolean withParseText, boolean withParseData) throws Exception {
    segmentDir = dir;
    this.nfs = nfs;
    if (isParsed) {
      fetcherReader = new ArrayFile.Reader(nfs, new File(dir, FetcherOutput.DIR_NAME).toString());
    } else {
      fetcherReader = new ArrayFile.Reader(nfs, new File(dir, FetcherOutput.DIR_NAME_NP).toString());
    }
    if (withContent) contentReader = new ArrayFile.Reader(nfs, new File(dir, Content.DIR_NAME).toString());
    if (withParseText) parseTextReader = new ArrayFile.Reader(nfs, new File(dir, ParseText.DIR_NAME).toString());
    if (withParseData) parseDataReader = new ArrayFile.Reader(nfs, new File(dir, ParseData.DIR_NAME).toString());
    // count the number of valid entries.
    // XXX We assume that all other data files contain the
    // XXX same number of valid entries - which is not always
    // XXX true if Fetcher crashed in the middle of update.
    // XXX One should check for this later, when actually
    // XXX reading the entries.
    FetcherOutput fo = new FetcherOutput();
    fetcherReader.next(fo);
    started = fo.getFetchDate();
    LongWritable w = new LongWritable(-1);
    try {
      fetcherReader.finalKey(w);
    } catch (Throwable eof) {
      // the file is truncated - probably due to a crashed fetcher.
      // Use just the part that we can...
      LOG.warning(" - data in segment " + dir + " is corrupt, using only " + w.get() + " entries.");
    }
    // go back until you get a good entry
    size = w.get()+1;
    boolean ok = false;
    int back = 0;
    do {
      try {
        fetcherReader.seek(size - 2 - back);
        fetcherReader.next(fo);
        ok = true;
      } catch (Throwable t) {
        back++;
      }
    } while (!ok && back < 10);
    if (back >= 10)
      throw new Exception(" - fetcher output is unreadable");
    if (back > 0) LOG.warning(" - fetcher output truncated by " + back + " to " + size);
    size = size - back;
    finished = fo.getFetchDate();
    // reposition to the start
    fetcherReader.reset();
  }

  /**
   * Get a specified entry from the segment. Note: even if some of the storage objects
   * are null, but if respective readers are open a seek(n) operation will be performed
   * anyway, to ensure that the whole entry is valid.
   * 
   * @param n position of the entry
   * @param fo storage for FetcherOutput data. Must not be null.
   * @param co storage for Content data, or null.
   * @param pt storage for ParseText data, or null.
   * @param pd storage for ParseData data, or null.
   * @return true if all requested data successfuly read, false otherwise
   * @throws IOException
   */
  public synchronized boolean get(long n, FetcherOutput fo, Content co,
          ParseText pt, ParseData pd) throws IOException {
    //XXX a trivial implementation would be to do the following:
    //XXX   seek(n);
    //XXX   return next(fo, co, pt, pd);
    //XXX However, get(long, Writable) may be more optimized
    boolean valid = true;
    if (fetcherReader.get(n, fo) == null) valid = false;
    if (contentReader != null) {
      if (co != null) {
        if (contentReader.get(n, co) == null) valid = false;
      } else contentReader.seek(n);
    }
    if (parseTextReader != null) {
      if (pt != null) {
        if (parseTextReader.get(n, pt) == null) valid = false;
      } else parseTextReader.seek(n);
    }
    if (parseDataReader != null) {
      if (pd != null) {
        if (parseDataReader.get(n, pd) == null) valid = false;
      } else parseDataReader.seek(n);
    }
    key = n;
    return valid;
  }
  
  private Content _co = new Content();
  private ParseText _pt = new ParseText();
  private ParseData _pd = new ParseData();
  
  /** Read values from all open readers. Note: even if some of the storage objects
   * are null, but if respective readers are open, an underlying next() operation will
   * be performed for all streams anyway, to ensure that the whole entry is valid.
   */
  public synchronized boolean next(FetcherOutput fo, Content co,
          ParseText pt, ParseData pd) throws IOException {
    boolean valid = true;
    Content rco = (co == null) ? _co : co;
    ParseText rpt = (pt == null) ? _pt : pt;
    ParseData rpd = (pd == null) ? _pd : pd;
    if (fetcherReader.next(fo) == null) valid = false;
    if (contentReader != null)
      if (contentReader.next(rco) == null) valid = false;
    if (parseTextReader != null)
      if (parseTextReader.next(rpt) == null) valid = false;
    if (parseDataReader != null)
      if (parseDataReader.next(rpd) == null) valid = false;
    key++;
    return valid;
  }
  
  /** Seek to a position in all readers. */
  public synchronized void seek(long n) throws IOException {
    fetcherReader.seek(n);
    if (contentReader != null) contentReader.seek(n);
    if (parseTextReader != null) parseTextReader.seek(n);
    if (parseDataReader != null) parseDataReader.seek(n);
    key = n;
  }

  /** Return the current key position. */
  public long key() {
    return key;
  }

  /** Reset all readers. */
  public synchronized void reset() throws IOException {
    fetcherReader.reset();
    if (contentReader != null) contentReader.reset();
    if (parseTextReader != null) parseTextReader.reset();
    if (parseDataReader != null) parseDataReader.reset();
  }

  /** Close all readers. */
  public synchronized void close() {
    try {
      fetcherReader.close();
    } catch (Exception e) {};
    if (contentReader != null) try {
      contentReader.close();
    } catch (Exception e) {};
    if (parseTextReader != null) try {
      parseTextReader.close();
    } catch (Exception e) {};
    if (parseDataReader != null) try {
      parseDataReader.close();
    } catch (Exception e) {};
  }
  
  /**
   * Dump the segment's content in human-readable format.
   * @param sorted if true, sort segment entries by URL (ascending). If false,
   * output entries in the order they occur in the segment.
   * @param output where to dump to
   * @throws Exception
   */
  public synchronized void dump(boolean sorted, PrintStream output) throws Exception {
    reset();
    FetcherOutput fo = new FetcherOutput();
    Content co = new Content();
    ParseData pd = new ParseData();
    ParseText pt = new ParseText();
    long recNo = 0L;
    if (!sorted) {
      while(next(fo, co, pt, pd)) {
        output.println("Recno:: " + recNo++);
        output.println("FetcherOutput::\n" + fo.toString());
        if (contentReader != null)
          output.println("Content::\n" + co.toString());
        if (parseDataReader != null)
          output.println("ParseData::\n" + pd.toString());
        if (parseTextReader != null)
          output.println("ParseText::\n" + pt.toString());
        output.println("");
      }
    } else {
      File unsortedFile = new File(segmentDir, ".unsorted");
      File sortedFile = new File(segmentDir, ".sorted");
      nfs.delete(unsortedFile);
      nfs.delete(sortedFile);
      SequenceFile.Writer seqWriter = new SequenceFile.Writer(nfs,
              unsortedFile.toString(), UTF8.class, LongWritable.class);
      FetchListEntry fle;
      LongWritable rec = new LongWritable();
      UTF8 url = new UTF8();
      String urlString;
      while (fetcherReader.next(fo) != null) {
        fle = fo.getFetchListEntry();
        urlString = fle.getPage().getURL().toString();
        rec.set(recNo);
        url.set(urlString);
        seqWriter.append(url, rec);
        recNo++;
      }
      seqWriter.close();
      // sort the SequenceFile
      long start = System.currentTimeMillis();

      SequenceFile.Sorter sorter = new SequenceFile.Sorter(nfs,
              new UTF8.Comparator(), LongWritable.class);

      sorter.sort(unsortedFile.toString(), sortedFile.toString());

      float localSecs = (System.currentTimeMillis() - start) / 1000.0f;
      LOG.info(" - sorted: " + recNo + " entries in " + localSecs + "s, "
        + (recNo/localSecs) + " entries/s");

      nfs.delete(unsortedFile);
      SequenceFile.Reader seqReader = new SequenceFile.Reader(nfs, sortedFile.toString());
      while (seqReader.next(url, rec)) {
        recNo = rec.get();
        get(recNo, fo, co, pt, pd);
        output.println("Recno:: " + recNo++);
        output.println("FetcherOutput::\n" + fo.toString());
        if (contentReader != null)
          output.println("Content::\n" + co.toString());
        if (parseDataReader != null)
          output.println("ParseData::\n" + pd.toString());
        if (parseTextReader != null)
          output.println("ParseText::\n" + pt.toString());
        output.println("");
      }
      seqReader.close();
      nfs.delete(sortedFile);
    }
  }

  /** Command-line wrapper. Run without arguments to see usage help. */
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      usage();
      return;
    }
    SegmentReader reader = null;
    NutchFileSystem nfs = NutchFileSystem.parseArgs(args, 0);
    String segDir = null;
    Vector dirs = new Vector();
    boolean fix = false;
    boolean list = false;
    boolean dump = false;
    boolean sorted = false;
    boolean withParseText = true;
    boolean withParseData = true;
    boolean withContent = true;
    for (int i = 0; i < args.length; i++) {
      if (args[i] != null) {
        if (args[i].equals("-noparsetext")) withParseText = false;
        else if (args[i].equals("-noparsedata")) withParseData = false;
        else if (args[i].equals("-nocontent")) withContent = false;
        else if (args[i].equals("-fix")) fix = true;
        else if (args[i].equals("-dump")) dump = true;
        else if (args[i].equals("-dumpsort")) {
          dump = true;
          sorted = true;
        } else if (args[i].equals("-list")) list = true;
        else if (args[i].equals("-dir")) segDir = args[++i];
        else dirs.add(new File(args[i]));
      }
    }
    if (segDir != null) {
      File sDir = new File(segDir);
      if (!sDir.exists() || !sDir.isDirectory()) {
        LOG.warning("Invalid path: " + sDir);
      } else {
        File[] files = sDir.listFiles(new FileFilter() {
          public boolean accept(File f) {
            return f.isDirectory();
          }
        });
        if (files != null && files.length > 0) {
          for (int i = 0; i < files.length; i++) dirs.add(files[i]);
        }
      }
    }
    if (dirs.size() == 0) {
      LOG.severe("No input segment dirs.");
      usage();
      return;
    }
    long total = 0L;
    int cnt = 0;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd'-'HH:mm:ss");
    DecimalFormat df = new DecimalFormat("########");
    df.setParseIntegerOnly(true);
    if (list)
      LOG.info("PARSED?\tSTARTED\t\t\tFINISHED\t\tCOUNT\tDIR NAME");
    for (int i = 0; i < dirs.size(); i++) {
      File dir = (File)dirs.get(i);
      try {
        reader = new SegmentReader(nfs, dir,
              withContent, withParseText, withParseData, fix);
        if (list) {
          LOG.info(reader.isParsed + 
                  "\t" + sdf.format(new Date(reader.started)) +
                  "\t" + sdf.format(new Date(reader.finished)) +
                  "\t" + df.format(reader.size) +
                  "\t" + dir);
        }
        total += reader.size;
        cnt++;
        if (dump) reader.dump(sorted, System.out);
      } catch (Throwable t) {
        t.printStackTrace();
        LOG.warning(t.getMessage());
      }
    }
    if (list)
      LOG.info("TOTAL: " + total + " entries in " + cnt + " segments.");
  }
  
  private static void usage() {
    System.err.println("SegmentReader [-fix] [-dump] [-dumpsort] [-list] [-nocontent] [-noparsedata] [-noparsetext] (-dir segments | seg1 seg2 ...)");
    System.err.println("\tNOTE: at least one segment dir name is required, or '-dir' option.");
    System.err.println("\t-fix\t\tautomatically fix corrupted segments");
    System.err.println("\t-dump\t\tdump segment data in human-readable format");
    System.err.println("\t-dumpsort\tdump segment data in human-readable format, sorted by URL");
    System.err.println("\t-list\t\tprint useful information about segments");
    System.err.println("\t-nocontent\tignore content data");
    System.err.println("\t-noparsedata\tignore parse_data data");
    System.err.println("\t-nocontent\tignore parse_text data");
    System.err.println("\t-dir segments\tdirectory containing multiple segments");
    System.err.println("\tseg1 seg2 ...\tsegment directories\n");
  }
}

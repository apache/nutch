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

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;

import org.apache.nutch.fetcher.FetcherOutput;
import org.apache.nutch.indexer.IndexSegment;
import org.apache.nutch.io.MD5Hash;
import org.apache.nutch.fs.*;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.segment.SegmentReader;
import org.apache.nutch.segment.SegmentWriter;
import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.util.NutchConf;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.DateField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;

/**
 * This class cleans up accumulated segments data, and merges them into a single
 *  (or optionally multiple) segment(s), with no duplicates in it.
 * 
 * <p>
 * There are no prerequisites for its correct
 * operation except for a set of already fetched segments (they don't have to
 * contain parsed content, only fetcher output is required). This tool does not
 * use DeleteDuplicates, but creates its own "master" index of all pages in all
 * segments. Then it walks sequentially through this index and picks up only
 * most recent versions of pages for every unique value of url or hash.
 * </p>
 * <p>If some of the input segments are corrupted, this tool will attempt to
 * repair them, using 
 * {@link org.apache.nutch.segment.SegmentReader#fixSegment(NutchFileSystem, File, boolean, boolean, boolean, boolean)} method.</p>
 * <p>Output segment can be optionally split on the fly into several segments of fixed
 * length.</p>
 * <p>
 * The newly created segment(s) can be then optionally indexed, so that it can be
 * either merged with more new segments, or used for searching as it is.
 * </p>
 * <p>
 * Old segments may be optionally removed, because all needed data has already
 * been copied to the new merged segment. NOTE: this tool will remove also all
 * corrupted input segments, which are not useable anyway - however, this option
 * may be dangerous if you inadvertently included non-segment directories as
 * input...</p>
 * <p>
 * You may want to run SegmentMergeTool instead of following the manual procedures,
 * with all options turned on, i.e. to merge segments into the output segment(s),
 * index it, and then delete the original segments data.
 * </p>
 * 
 * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
 */
public class SegmentMergeTool implements Runnable {

  public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.tools.SegmentMergeTool");

  /** Log progress update every LOG_STEP items. */
  public static int LOG_STEP = 20000;
  /** Temporary de-dup index size. Larger indexes tend to slow down indexing.
   * Too many indexes slow down the subsequent index merging. It's a tradeoff value...
   */
  public static int INDEX_SIZE = 250000;
  public static int INDEX_MERGE_FACTOR = 30;
  public static int INDEX_MIN_MERGE_DOCS = 100;
  
  private boolean boostByLinkCount =
    NutchConf.get().getBoolean("indexer.boost.by.link.count", false);

  private float scorePower = NutchConf.get().getFloat("indexer.score.power", 0.5f);
  
  private NutchFileSystem nfs = null;
  private File[] segments = null;
  private int stage = SegmentMergeStatus.STAGE_OPENING;
  private long totalRecords = 0L;
  private long processedRecords = 0L;
  private long start = 0L;
  private long maxCount = Long.MAX_VALUE;
  private File output = null;
  private List segdirs = null;
  private List allsegdirs = null;
  private boolean runIndexer = false;
  private boolean delSegs = false;
  private HashMap readers = new HashMap();

  /**
   * Create a SegmentMergeTool.
   * @param nfs filesystem
   * @param segments list of input segments
   * @param output output directory, where output segments will be created
   * @param maxCount maximum number of records per output segment. If this
   * value is 0, then the default value {@link Long#MAX_VALUE} is used.
   * @param runIndexer run indexer on output segment(s)
   * @param delSegs delete input segments when finished
   * @throws Exception
   */
  public SegmentMergeTool(NutchFileSystem nfs, File[] segments, File output, long maxCount, boolean runIndexer, boolean delSegs) throws Exception {
    this.nfs = nfs;
    this.segments = segments;
    this.runIndexer = runIndexer;
    this.delSegs = delSegs;
    if (maxCount > 0) this.maxCount = maxCount;
    allsegdirs = Arrays.asList(segments);
    this.output = output;
    if (nfs.exists(output)) {
      if (!nfs.isDirectory(output))
        throw new Exception("Output is not a directory: " + output);
    } else nfs.mkdirs(output);
  }

  public static class SegmentMergeStatus {
    public static final int STAGE_OPENING     = 0;
    public static final int STAGE_MASTERIDX   = 1;
    public static final int STAGE_MERGEIDX    = 2;
    public static final int STAGE_DEDUP       = 3;
    public static final int STAGE_WRITING     = 4;
    public static final int STAGE_INDEXING    = 5;
    public static final int STAGE_DELETING    = 6;
    public static final String[] stages = {
            "opening input segments",
            "creating master index",
            "merging sub-indexes",
            "deduplicating",
            "writing output segment(s)",
            "indexing output segment(s)",
            "deleting input segments"
    };
    public int stage;
    public File[] inputSegments;
    public long startTime, curTime;
    public long totalRecords;
    public long processedRecords;
    
    public SegmentMergeStatus() {};
    
    public SegmentMergeStatus(int stage, File[] inputSegments, long startTime,
            long totalRecords, long processedRecords) {
      this.stage = stage;
      this.inputSegments = inputSegments;
      this.startTime = startTime;
      this.curTime = System.currentTimeMillis();
      this.totalRecords = totalRecords;
      this.processedRecords = processedRecords;
    }    
  }
  
  public SegmentMergeStatus getStatus() {
    SegmentMergeStatus status = new SegmentMergeStatus(stage, segments, start,
            totalRecords, processedRecords);
    return status;
  }
  
  /** Run the tool, periodically reporting progress. */
  public void run() {
    start = System.currentTimeMillis();
    stage = SegmentMergeStatus.STAGE_OPENING;
    long delta;
    LOG.info("* Opening " + allsegdirs.size() + " segments:");
    try {
      segdirs = new ArrayList();
      // open all segments
      for (int i = 0; i < allsegdirs.size(); i++) {
        File dir = (File) allsegdirs.get(i);
        SegmentReader sr = null;
        try {
          // try to autofix it if corrupted...
          sr = new SegmentReader(nfs, dir, true);
        } catch (Exception e) {
          // this segment is hosed beyond repair, don't use it
          LOG.warning("* Segment " + dir.getName() + " is corrupt beyond repair; skipping it.");
          continue;
        }
        segdirs.add(dir);
        totalRecords += sr.size;
        LOG.info(" - segment " + dir.getName() + ": " + sr.size + " records.");
        readers.put(dir.getName(), sr);
      }
      long total = totalRecords;
      LOG.info("* TOTAL " + total + " input records in " + segdirs.size() + " segments.");
      LOG.info("* Creating master index...");
      stage = SegmentMergeStatus.STAGE_MASTERIDX;
      // XXX Note that Lucene indexes don't work with NutchFileSystem for now.
      // XXX For now always assume LocalFileSystem here...
      Vector masters = new Vector();
      File fsmtIndexDir = new File(output, ".fastmerge_index");
      File masterDir = new File(fsmtIndexDir, "0");
      if (!masterDir.mkdirs()) {
        LOG.severe("Could not create a master index dir: " + masterDir);
        return;
      }
      masters.add(masterDir);
      IndexWriter iw = new IndexWriter(masterDir, new WhitespaceAnalyzer(), true);
      iw.setUseCompoundFile(false);
      iw.mergeFactor = INDEX_MERGE_FACTOR;
      iw.minMergeDocs = INDEX_MIN_MERGE_DOCS;
      long s1 = System.currentTimeMillis();
      Iterator it = readers.values().iterator();
      processedRecords = 0L;
      delta = System.currentTimeMillis();
      while (it.hasNext()) {
        SegmentReader sr = (SegmentReader) it.next();
        String name = sr.segmentDir.getName();
        FetcherOutput fo = new FetcherOutput();
        for (long i = 0; i < sr.size; i++) {
          try {
            if (!sr.get(i, fo, null, null, null)) break;

            Document doc = new Document();
            
            // compute boost
            float boost = IndexSegment.calculateBoost(fo.getFetchListEntry().getPage().getScore(),
                    scorePower, boostByLinkCount, fo.getAnchors().length);
            doc.add(new Field("sd", name + "|" + i, true, false, false));
            doc.add(new Field("uh", MD5Hash.digest(fo.getUrl().toString()).toString(), true, true, false));
            doc.add(new Field("ch", fo.getMD5Hash().toString(), true, true, false));
            doc.add(new Field("time", DateField.timeToString(fo.getFetchDate()), true, false, false));
            doc.add(new Field("score", boost + "", true, false, false));
            doc.add(new Field("ul", fo.getUrl().toString().length() + "", true, false, false));
            iw.addDocument(doc);
            processedRecords++;
            if (processedRecords > 0 && (processedRecords % LOG_STEP == 0)) {
              LOG.info(" Processed " + processedRecords + " records (" +
                      (float)(LOG_STEP * 1000)/(float)(System.currentTimeMillis() - delta) + " rec/s)");
              delta = System.currentTimeMillis();
            }
            if (processedRecords > 0 && (processedRecords % INDEX_SIZE == 0)) {
              iw.optimize();
              iw.close();
              LOG.info(" - creating next subindex...");
              masterDir = new File(fsmtIndexDir, "" + masters.size());
              if (!masterDir.mkdirs()) {
                LOG.severe("Could not create a master index dir: " + masterDir);
                return;
              }
              masters.add(masterDir);
              iw = new IndexWriter(masterDir, new WhitespaceAnalyzer(), true);
              iw.setUseCompoundFile(false);
              iw.mergeFactor = INDEX_MERGE_FACTOR;
              iw.minMergeDocs = INDEX_MIN_MERGE_DOCS;
            }
          } catch (Throwable t) {
            // we can assume the data is invalid from now on - break here
            LOG.info(" - segment " + name + " truncated to " + (i + 1) + " records");
            break;
          }
        }
      }
      iw.optimize();
      LOG.info("* Creating index took " + (System.currentTimeMillis() - s1) + " ms");
      s1 = System.currentTimeMillis();
      // merge all other indexes using the latest IndexWriter (still open):
      if (masters.size() > 1) {
        LOG.info(" - merging subindexes...");
        stage = SegmentMergeStatus.STAGE_MERGEIDX;
        IndexReader[] ireaders = new IndexReader[masters.size() - 1];
        for (int i = 0; i < masters.size() - 1; i++) ireaders[i] = IndexReader.open((File)masters.get(i));
        iw.addIndexes(ireaders);
        for (int i = 0; i < masters.size() - 1; i++) {
          ireaders[i].close();
          FileUtil.fullyDelete((File)masters.get(i));
        }
      }
      iw.close();
      LOG.info("* Optimizing index took " + (System.currentTimeMillis() - s1) + " ms");
      LOG.info("* Removing duplicate entries...");
      stage = SegmentMergeStatus.STAGE_DEDUP;
      IndexReader ir = IndexReader.open(masterDir);
      int i = 0;
      long cnt = 0L;
      processedRecords = 0L;
      s1 = System.currentTimeMillis();
      delta = s1;
      TermEnum te = ir.terms();
      while(te.next()) {
        Term t = te.term();
        if (t == null) continue;
        if (!(t.field().equals("ch") || t.field().equals("uh"))) continue;
        cnt++;
        processedRecords = cnt / 2;
        if (cnt > 0 && (cnt % (LOG_STEP  * 2) == 0)) {
          LOG.info(" Processed " + processedRecords + " records (" +
                  (float)(LOG_STEP * 1000)/(float)(System.currentTimeMillis() - delta) + " rec/s)");
          delta = System.currentTimeMillis();
        }
        // Enumerate all docs with the same URL hash or content hash
        TermDocs td = ir.termDocs(t);
        if (td == null) continue;
        if (t.field().equals("uh")) {
          // Keep only the latest version of the document with
          // the same url hash. Note: even if the content
          // hash is identical, other metadata may be different, so even
          // in this case it makes sense to keep the latest version.
          int id = -1;
          String time = null;
          Document doc = null;
          while (td.next()) {
            int docid = td.doc();
            if (!ir.isDeleted(docid)) {
              doc = ir.document(docid);
              if (time == null) {
                time = doc.get("time");
                id = docid;
                continue;
              }
              String dtime = doc.get("time");
              // "time" is a DateField, and can be compared lexicographically
              if (dtime.compareTo(time) > 0) {
                if (id != -1) {
                  ir.delete(id);
                }
                time = dtime;
                id = docid;
              } else {
                ir.delete(docid);
              }
            }
          }
        } else if (t.field().equals("ch")) {
          // Keep only the version of the document with
          // the highest score, and then with the shortest url.
          int id = -1;
          int ul = 0;
          float score = 0.0f;
          Document doc = null;
          while (td.next()) {
            int docid = td.doc();
            if (!ir.isDeleted(docid)) {
              doc = ir.document(docid);
              if (ul == 0) {
                try {
                  ul = Integer.parseInt(doc.get("ul"));
                  score = Float.parseFloat(doc.get("score"));
                } catch (Exception e) {};
                id = docid;
                continue;
              }
              int dul = 0;
              float dscore = 0.0f;
              try {
                dul = Integer.parseInt(doc.get("ul"));
                dscore = Float.parseFloat(doc.get("score"));
              } catch (Exception e) {};
              int cmp = Float.compare(dscore, score);
              if (cmp == 0) {
                // equal scores, select the one with shortest url
                if (dul < ul) {
                  if (id != -1) {
                    ir.delete(id);
                  }
                  ul = dul;
                  id = docid;
                } else {
                  ir.delete(docid);
                }
              } else if (cmp < 0) {
                ir.delete(docid);
              } else {
                if (id != -1) {
                  ir.delete(id);
                }
                ul = dul;
                id = docid;
              }
            }
          }
        }
      }
      //
      // keep the IndexReader open...
      //
      
      LOG.info("* Deduplicating took " + (System.currentTimeMillis() - s1) + " ms");
      stage = SegmentMergeStatus.STAGE_WRITING;
      processedRecords = 0L;
      Vector outDirs = new Vector();
      File outDir = new File(output, SegmentWriter.getNewSegmentName());
      outDirs.add(outDir);
      LOG.info("* Merging all segments into " + output.getName());
      s1 = System.currentTimeMillis();
      delta = s1;
      nfs.mkdirs(outDir);
      SegmentWriter sw = new SegmentWriter(nfs, outDir, true);
      LOG.fine(" - opening first output segment in " + outDir.getName());
      FetcherOutput fo = new FetcherOutput();
      Content co = new Content();
      ParseText pt = new ParseText();
      ParseData pd = new ParseData();
      int outputCnt = 0;
      for (int n = 0; n < ir.maxDoc(); n++) {
        if (ir.isDeleted(n)) {
          //System.out.println("-del");
          continue;
        }
        Document doc = ir.document(n);
        String segDoc = doc.get("sd");
        int idx = segDoc.indexOf('|');
        String segName = segDoc.substring(0, idx);
        String docName = segDoc.substring(idx + 1);
        SegmentReader sr = (SegmentReader) readers.get(segName);
        long docid;
        try {
          docid = Long.parseLong(docName);
        } catch (Exception e) {
          continue;
        }
        try {
          // get data from the reader
          sr.get(docid, fo, co, pt, pd);
        } catch (Throwable thr) {
          // don't break the loop, because only one of the segments
          // may be corrupted...
          LOG.fine(" - corrupt record no. " + docid + " in segment " + sr.segmentDir.getName() + " - skipping.");
          continue;
        }
        sw.append(fo, co, pt, pd);
        outputCnt++;
        processedRecords++;
        if (processedRecords > 0 && (processedRecords % LOG_STEP == 0)) {
          LOG.info(" Processed " + processedRecords + " records (" +
                  (float)(LOG_STEP * 1000)/(float)(System.currentTimeMillis() - delta) + " rec/s)");
          delta = System.currentTimeMillis();
        }
        if (processedRecords % maxCount == 0) {
          sw.close();
          outDir = new File(output, SegmentWriter.getNewSegmentName());
          LOG.fine(" - starting next output segment in " + outDir.getName());
          nfs.mkdirs(outDir);
          sw = new SegmentWriter(nfs, outDir, true);
          outDirs.add(outDir);
        }
      }
      LOG.info("* Merging took " + (System.currentTimeMillis() - s1) + " ms");
      ir.close();
      sw.close();
      FileUtil.fullyDelete(fsmtIndexDir);
      for (Iterator iter = readers.keySet().iterator(); iter.hasNext();) {
        SegmentReader sr = (SegmentReader) readers.get(iter.next());
        sr.close();
      }
      if (runIndexer) {
        stage = SegmentMergeStatus.STAGE_INDEXING;
        totalRecords = outDirs.size();
        processedRecords = 0L;
        LOG.info("* Creating new segment index(es)...");
        File workingDir = new File(output, "indexsegment-workingdir");
        for (int k = 0; k < outDirs.size(); k++) {
          processedRecords++;
          if (workingDir.exists()) {
              FileUtil.fullyDelete(workingDir);
          }
          IndexSegment indexer = new IndexSegment(nfs, Integer.MAX_VALUE,
                  (File)outDirs.get(k), workingDir);
          indexer.indexPages();
          FileUtil.fullyDelete(workingDir);
        }
      }
      if (delSegs) {
        // This deletes also all corrupt segments, which are
        // unusable anyway
        stage = SegmentMergeStatus.STAGE_DELETING;
        totalRecords = allsegdirs.size();
        processedRecords = 0L;
        LOG.info("* Deleting old segments...");
        for (int k = 0; k < allsegdirs.size(); k++) {
          processedRecords++;
          FileUtil.fullyDelete((File) allsegdirs.get(k));
        }
      }
      delta = System.currentTimeMillis() - start;
      float eps = (float) total / (float) (delta / 1000);
      LOG.info("Finished SegmentMergeTool: INPUT: " + total + " -> OUTPUT: " + outputCnt + " entries in "
              + ((float) delta / 1000f) + " s (" + eps + " entries/sec).");
    } catch (Exception e) {
      e.printStackTrace();
      LOG.severe(e.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Too few arguments.\n");
      usage();
      System.exit(-1);
    }
    NutchFileSystem nfs = NutchFileSystem.parseArgs(args, 0);
    boolean runIndexer = false;
    boolean delSegs = false;
    long maxCount = Long.MAX_VALUE;
    String segDir = null;
    File output = null;
    Vector dirs = new Vector();
    for (int i = 0; i < args.length; i++) {
      if (args[i] == null) continue;
      if (args[i].equals("-o")) {
        if (args.length > i + 1) {
          output = new File(args[++i]);
          continue;
        } else {
          LOG.severe("Required value of '-o' argument missing.\n");
          usage();
          return;
        }
      } else if (args[i].equals("-i")) {
        runIndexer = true;
      } else if (args[i].equals("-cm")) {
        LOG.warning("'-cm' option obsolete - ignored.");
      } else if (args[i].equals("-max")) {
        String cnt = args[++i];
        try {
          maxCount = Long.parseLong(cnt);
        } catch (Exception e) {
          LOG.warning("Invalid count '" + cnt + "', setting to Long.MAX_VALUE.");
        }
      } else if (args[i].equals("-ds")) {
        delSegs = true;
      } else if (args[i].equals("-dir")) {
        segDir = args[++i];
      } else dirs.add(new File(args[i]));
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
      LOG.severe("No input segments.");
      return;
    }
    if (output == null) output = ((File)dirs.get(0)).getParentFile();
    SegmentMergeTool st = new SegmentMergeTool(nfs, (File[])dirs.toArray(new File[0]),
            output, maxCount, runIndexer, delSegs);
    st.run();
  }

  private static void usage() {
    System.err.println("SegmentMergeTool (-local | -nfs ...) (-dir <input_segments_dir> | seg1 seg2 ...) [-o <output_segments_dir>] [-max count] [-i] [-ds]");
    System.err.println("\t-dir <input_segments_dir>\tpath to directory containing input segments");
    System.err.println("\tseg1 seg2 seg3\t\tindividual paths to input segments");
    System.err.println("\t-o <output_segment_dir>\t(optional) path to directory which will\n\t\t\t\tcontain output segment(s).\n\t\t\tNOTE: If not present, the original segments path will be used.");
    System.err.println("\t-max count\t(optional) output multiple segments, each with maximum 'count' entries");
    System.err.println("\t-i\t\t(optional) index the output segment when finished merging.");
    System.err.println("\t-ds\t\t(optional) delete the original input segments when finished.");
    System.err.println();
  }
}

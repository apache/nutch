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

package org.apache.nutch.indexer;

import org.apache.nutch.pagedb.*;
import org.apache.nutch.linkdb.*;
import org.apache.nutch.fetcher.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.analysis.NutchDocumentAnalyzer;
import org.apache.nutch.db.*;
import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.segment.SegmentReader;
import org.apache.nutch.util.*;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.util.logging.*;
import java.util.*;
import java.io.*;

/** Creates an index for the output corresponding to a single fetcher run. */
public class IndexSegment {
  public static final String DONE_NAME = "index.done";
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.index.IndexSegment");
  
  public static int LOG_STEP = 20000;

  private boolean boostByLinkCount =
    NutchConf.get().getBoolean("indexer.boost.by.link.count", false);

  private float scorePower = NutchConf.get().getFloat("indexer.score.power", 0.5f);
  private int maxFieldLength = NutchConf.get().getInt("indexer.max.tokens", 10000);
  private int MERGE_FACTOR = NutchConf.get().getInt("indexer.mergeFactor",
      IndexWriter.DEFAULT_MERGE_FACTOR);
  private int MIN_MERGE_DOCS = NutchConf.get().getInt("indexer.minMergeDocs",
      IndexWriter.DEFAULT_MIN_MERGE_DOCS);
  private int MAX_MERGE_DOCS = NutchConf.get().getInt("indexer.maxMergeDocs",
      IndexWriter.DEFAULT_MAX_MERGE_DOCS);
  private NutchFileSystem nfs;
  private long maxDocs = Long.MAX_VALUE;
  private File srcDir;
  private File localWorkingDir;

  /**
   * Index a segment in the given NFS.
   */
  public IndexSegment(NutchFileSystem nfs, long maxDocs, File srcDir, File localWorkingDir) {
      this.nfs = nfs;
      this.maxDocs = maxDocs;
      this.srcDir = srcDir;
      this.localWorkingDir = localWorkingDir;
  }

  /** Determines the power of link analyis scores.  Each pages's boost is
   * set to <i>score<sup>scorePower</sup></i> where <i>score</i> is its link
   * analysis score and <i>scorePower</i> is the value passed to this method.
   */
  public void setScorePower(float power) { scorePower = power; }

  public void indexPages() throws Exception {
      //
      // First, see if it's ever been indexed before
      //
      File doneFile = new File(srcDir, DONE_NAME);
      if (nfs.exists(doneFile)) {
          throw new IOException("already indexed: " + doneFile + " exists");
      }

      //
      // OK, fine.  Build the writer to the local file, set params
      //
      File outputIndex = new File(srcDir, "index");
      File tmpOutputIndex = new File(localWorkingDir, "index");

      File localOutput = nfs.startLocalOutput(outputIndex, tmpOutputIndex);

      IndexWriter writer
          = new IndexWriter(localOutput, 
                            new NutchDocumentAnalyzer(), true);
      writer.mergeFactor = MERGE_FACTOR;
      writer.minMergeDocs = MIN_MERGE_DOCS;
      writer.maxMergeDocs = MAX_MERGE_DOCS;
      writer.maxFieldLength = maxFieldLength;
      //writer.infoStream = LogFormatter.getLogStream(LOG, Level.FINE);
      writer.setUseCompoundFile(false);
      writer.setSimilarity(new NutchSimilarity());

      SegmentReader sr = null;

      long start = System.currentTimeMillis();
      long delta = start;
      long curTime, total = 0;
      long count = 0;
      try {
          LOG.info("* Opening segment " + srcDir.getName());
          sr = new SegmentReader(nfs, srcDir, false, true, true, true);

          total = sr.size;
          
          String segmentName = srcDir.getCanonicalFile().getName();
          FetcherOutput fetcherOutput = new FetcherOutput();
          ParseText parseText = new ParseText();
          ParseData parseData = new ParseData();
          LOG.info("* Indexing segment " + srcDir.getName());

          //
          // Iterate through all docs in the input
          //
          maxDocs = Math.min(sr.size, maxDocs);
          for (count = 0; count < maxDocs; count++) {
            if (!sr.next(fetcherOutput, null, parseText, parseData)) continue;

              // only index the page if it was fetched correctly
              if (fetcherOutput.getStatus() != FetcherOutput.SUCCESS) {
                  continue;                              
              }

              // reconstruct parse
              Parse parse = new ParseImpl(parseText.getText(), parseData);

              // build initial document w/ core fields
              Document doc = makeDocument(segmentName, count,
                                          fetcherOutput, parse);

              // run filters to add more fields to the document
              doc = IndexingFilters.filter(doc, parse, fetcherOutput);
    
              // add the document to the index
              writer.addDocument(doc);
              if (count > 0 && count % LOG_STEP == 0) {
                curTime = System.currentTimeMillis();
                LOG.info(" Processed " + count + " records (" +
                        ((float)LOG_STEP * 1000.0f / (float)(curTime - delta)) +
                        " rec/s)");
                delta = curTime;
              }
          }
      } catch (EOFException e) {
          LOG.warning("Unexpected EOF in: " + srcDir +
                      " at entry #" + count + ".  Ignoring.");
      } finally {
        sr.close();
      }
      LOG.info("* Optimizing index...");
      writer.optimize();
      writer.close();

      //
      // Put the local file in its place via NFS
      //
      //nfs.completeLocalOutput(new File(outputDir, "index"), new File(srcDir, "index"));
      LOG.info("* Moving index to NFS if needed...");
      nfs.completeLocalOutput(outputIndex, tmpOutputIndex);

      //
      // Emit "done" file
      //
      OutputStream out = nfs.create(doneFile);
      out.close();
      delta = System.currentTimeMillis() - start;
      float eps = (float) count / (float) (delta / 1000);
      LOG.info("DONE indexing segment " + srcDir.getName() + ": total " + total +
              " records in " + ((float) delta / 1000f) + " s (" + eps + " rec/s).");
  }

  /** 
   * Add core fields, required by other core components & features (i.e.,
   * merge, dedup, explain). 
   */
  private Document makeDocument(String segmentName, long docNo,
                                FetcherOutput fo, Parse parse) {

    Document doc = new Document();

    // add docno & segment, used to map from merged index back to segment files
    doc.add(Field.UnIndexed("docNo", Long.toString(docNo, 16)));
    doc.add(Field.UnIndexed("segment", segmentName));

    // add digest, used by dedup
    doc.add(Field.UnIndexed("digest", fo.getMD5Hash().toString()));

    // compute boost
    // 1. Start with page's score from DB -- 1.0 if no link analysis.
    float boost = fo.getFetchListEntry().getPage().getScore();
    // 2. Apply scorePower to this.
    boost = (float)Math.pow(boost, scorePower);
    // 3. Optionally boost by log of incoming anchor count.
    if (boostByLinkCount)
      boost *= (float)Math.log(Math.E + fo.getAnchors().length);
    // 4. Apply boost to all indexed fields.
    doc.setBoost(boost);

    // store boost for use by explain and dedup
    doc.add(Field.UnIndexed("boost", Float.toString(boost)));

    return doc;
  }


  /** 
   * Create an index for the input files in the named directory. 
   */
  public static void main(String[] args) throws Exception {
      String usage = "IndexSegment (-local | -ndfs <namenode:port>) <segment_directory> [-dir <workingdir>]";
      if (args.length == 0) {
          System.err.println("Usage: " + usage);
          return;
      }

      NutchFileSystem nfs = NutchFileSystem.parseArgs(args, 0);
      try {
          int maxDocs = Integer.MAX_VALUE;
          File srcDir = null;
          File workingDir = new File(new File("").getCanonicalPath());
          for (int i = 0; i < args.length; i++) {
              if (args[i] != null) {
                  if (args[i].equals("-max")) {        // parse -max option
                      i++;
                      maxDocs = Integer.parseInt(args[i]);
                  } else if (args[i].equals("-dir")) {
                      i++;
                      workingDir = new File(new File(args[i]).getCanonicalPath());
                  } else {
                      srcDir = new File(args[i]);
                  }
              }
          }

          workingDir = new File(workingDir, "indexsegment-workingdir");
          if (workingDir.exists()) {
              FileUtil.fullyDelete(workingDir);
          }
          IndexSegment indexer = new IndexSegment(nfs, maxDocs, srcDir, workingDir);
          LOG.info("indexing segment: " + srcDir);
          indexer.indexPages();
          LOG.info("done indexing");
          FileUtil.fullyDelete(workingDir);
      } finally {
          nfs.close();
      }
  }
}

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

import java.io.*;
import java.text.*;
import java.util.*;
import java.util.logging.*;

import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.IndexWriter;

/*************************************************************************
 * IndexMerger creates an index for the output corresponding to a 
 * single fetcher run.
 * 
 * @author Doug Cutting
 * @author Mike Cafarella
 *************************************************************************/
public class IndexMerger {
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.indexer.IndexMerger");

  public static final String DONE_NAME = "merge.done";

  private NutchFileSystem nfs;
  private File outputIndex;
  private File localWorkingDir;
  private File[] indexes;
  private NutchConf nutchConf;

  /**
   * Merge all of the indexes given
   */
  public IndexMerger(NutchFileSystem nfs, File[] indexes, File outputIndex, File localWorkingDir, NutchConf nutchConf) throws IOException {
      this.nfs = nfs;
      this.indexes = indexes;
      this.outputIndex = outputIndex;
      this.localWorkingDir = localWorkingDir;
      this.nutchConf = nutchConf;
  }

  /**
   * All all input indexes to the single output index
   */
  public void merge() throws IOException {
    //
    // Open local copies of NFS indices
    //

    // Get local output target
    //
    File tmpLocalOutput = new File(localWorkingDir, "merge-output");
    File localOutput = nfs.startLocalOutput(outputIndex, tmpLocalOutput);

    Directory[] dirs = new Directory[indexes.length];
    for (int i = 0; i < indexes.length; i++) {
      LOG.info("Adding " + indexes[i]);
      dirs[i] = new NdfsDirectory(nfs, indexes[i], false, this.nutchConf);
    }

    //

    //
    // Merge indices
    //
    IndexWriter writer = new IndexWriter(localOutput, null, true);
    writer.mergeFactor = nutchConf.getInt("indexer.mergeFactor", IndexWriter.DEFAULT_MERGE_FACTOR);
    writer.minMergeDocs = nutchConf.getInt("indexer.minMergeDocs", IndexWriter.DEFAULT_MIN_MERGE_DOCS);
    writer.maxMergeDocs = nutchConf.getInt("indexer.maxMergeDocs", IndexWriter.DEFAULT_MAX_MERGE_DOCS);
    writer.setTermIndexInterval(nutchConf.getInt("indexer.termIndexInterval", IndexWriter.DEFAULT_TERM_INDEX_INTERVAL));
    writer.infoStream = LogFormatter.getLogStream(LOG, Level.FINE);
    writer.setUseCompoundFile(false);
    writer.setSimilarity(new NutchSimilarity());
    writer.addIndexes(dirs);
    writer.close();

    //
    // Put target back
    //
    nfs.completeLocalOutput(outputIndex, tmpLocalOutput);

    localWorkingDir.delete();
  }

  /** 
   * Create an index for the input files in the named directory. 
   */
  public static void main(String[] args) throws Exception {
    String usage = "IndexMerger [-workingdir <workingdir>] outputIndex indexesDir...";
    if (args.length < 2) {
      System.err.println("Usage: " + usage);
      return;
    }

    //
    // Parse args, read all index directories to be processed
    //
    NutchConf nutchConf = new NutchConf();
    NutchFileSystem nfs = NutchFileSystem.get(nutchConf);
    File workDir = new File(new File("").getCanonicalPath());
    List indexDirs = new ArrayList();

    int i = 0;
    if ("-workingdir".equals(args[i])) {
      i++;
      workDir = new File(new File(args[i++]).getCanonicalPath());
    }
    workDir = new File(workDir, "indexmerger-workingdir");

    File outputIndex = new File(args[i++]);

    for (; i < args.length; i++) {
      indexDirs.addAll(Arrays.asList(nfs.listFiles(new File(args[i]))));
    }

    //
    // Merge the indices
    //
    LOG.info("merging indexes to: " + outputIndex);

    File[] indexFiles = (File[])indexDirs.toArray(new File[indexDirs.size()]);

    if (workDir.exists()) {
      FileUtil.fullyDelete(workDir, nutchConf);
    }
    workDir.mkdirs();
    IndexMerger merger = new IndexMerger(nfs,indexFiles,outputIndex,workDir, nutchConf);
    merger.merge();
    LOG.info("done merging");
    FileUtil.fullyDelete(workDir, nutchConf);
  }
}

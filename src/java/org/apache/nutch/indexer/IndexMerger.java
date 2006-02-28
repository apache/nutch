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
import java.util.*;
import java.util.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;

import org.apache.nutch.util.NutchConfiguration;

import org.apache.lucene.store.Directory;
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

  private FileSystem fs;
  private File outputIndex;
  private File localWorkingDir;
  private File[] indexes;
  private Configuration conf;

  /**
   * Merge all of the indexes given
   */
  public IndexMerger(FileSystem fs, File[] indexes, File outputIndex, File localWorkingDir, Configuration conf) throws IOException {
      this.fs = fs;
      this.indexes = indexes;
      this.outputIndex = outputIndex;
      this.localWorkingDir = localWorkingDir;
      this.conf = conf;
  }

  /**
   * All all input indexes to the single output index
   */
  public void merge() throws IOException {
    //
    // Open local copies of FS indices
    //

    // Get local output target
    //
    File tmpLocalOutput = new File(localWorkingDir, "merge-output");
    File localOutput = fs.startLocalOutput(outputIndex, tmpLocalOutput);

    Directory[] dirs = new Directory[indexes.length];
    for (int i = 0; i < indexes.length; i++) {
      LOG.info("Adding " + indexes[i]);
      dirs[i] = new FsDirectory(fs, indexes[i], false, this.conf);
    }

    //

    //
    // Merge indices
    //
    IndexWriter writer = new IndexWriter(localOutput, null, true);
    writer.mergeFactor = conf.getInt("indexer.mergeFactor", IndexWriter.DEFAULT_MERGE_FACTOR);
    writer.minMergeDocs = conf.getInt("indexer.minMergeDocs", IndexWriter.DEFAULT_MIN_MERGE_DOCS);
    writer.maxMergeDocs = conf.getInt("indexer.maxMergeDocs", IndexWriter.DEFAULT_MAX_MERGE_DOCS);
    writer.setTermIndexInterval(conf.getInt("indexer.termIndexInterval", IndexWriter.DEFAULT_TERM_INDEX_INTERVAL));
    writer.infoStream = LogFormatter.getLogStream(LOG, Level.FINE);
    writer.setUseCompoundFile(false);
    writer.setSimilarity(new NutchSimilarity());
    writer.addIndexes(dirs);
    writer.close();

    //
    // Put target back
    //
    fs.completeLocalOutput(outputIndex, tmpLocalOutput);

    localWorkingDir.delete();
  }

  /** 
   * Create an index for the input files in the named directory. 
   */
  public static boolean doMain(String[] args) throws Exception {
    String usage = "IndexMerger [-workingdir <workingdir>] outputIndex indexesDir...";
    if (args.length < 2) {
      System.err.println("Usage: " + usage);
      return false;
    }

    //
    // Parse args, read all index directories to be processed
    //
    Configuration conf = NutchConfiguration.create();
    FileSystem fs = FileSystem.get(conf);
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
      indexDirs.addAll(Arrays.asList(fs.listFiles(new File(args[i]))));
    }

    //
    // Merge the indices
    //
    LOG.info("merging indexes to: " + outputIndex);

    File[] indexFiles = (File[])indexDirs.toArray(new File[indexDirs.size()]);

    if (workDir.exists()) {
      FileUtil.fullyDelete(workDir, conf);
    }
    workDir.mkdirs();
    IndexMerger merger = new IndexMerger(fs,indexFiles,outputIndex,workDir, conf);
    merger.merge();
    LOG.info("done merging");
    FileUtil.fullyDelete(workDir, conf);

    return true;
  }

  /**
   * main() wrapper that returns proper exit status
   */
  public static void main(String[] args) {
    Runtime rt = Runtime.getRuntime();
    try {
      boolean status = doMain(args);
      rt.exit(status ? 0 : 1);
    }
    catch (Exception e) {
      LOG.log(Level.SEVERE, "error, caught Exception in main()", e);
      rt.exit(1);
    }
  }
}

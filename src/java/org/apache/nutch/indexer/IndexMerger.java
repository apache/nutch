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
  private Path outputIndex;
  private Path localWorkingDir;
  private Path[] indexes;
  private Configuration conf;

  /**
   * Merge all of the indexes given
   */
  public IndexMerger(FileSystem fs, Path[] indexes, Path outputIndex, Path localWorkingDir, Configuration conf) throws IOException {
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
    Path tmpLocalOutput = new Path(localWorkingDir, "merge-output");
    Path localOutput = fs.startLocalOutput(outputIndex, tmpLocalOutput);

    Directory[] dirs = new Directory[indexes.length];
    for (int i = 0; i < indexes.length; i++) {
      LOG.info("Adding " + indexes[i]);
      dirs[i] = new FsDirectory(fs, indexes[i], false, this.conf);
    }

    //

    //
    // Merge indices
    //
    IndexWriter writer = new IndexWriter(localOutput.toString(), null, true);
    writer.setMergeFactor(conf.getInt("indexer.mergeFactor", IndexWriter.DEFAULT_MERGE_FACTOR));
    writer.setMaxBufferedDocs(conf.getInt("indexer.minMergeDocs", IndexWriter.DEFAULT_MAX_BUFFERED_DOCS));
    writer.setMaxMergeDocs(conf.getInt("indexer.maxMergeDocs", IndexWriter.DEFAULT_MAX_MERGE_DOCS));
    writer.setTermIndexInterval(conf.getInt("indexer.termIndexInterval", IndexWriter.DEFAULT_TERM_INDEX_INTERVAL));
    writer.setInfoStream(LogFormatter.getLogStream(LOG, Level.FINE));
    writer.setUseCompoundFile(false);
    writer.setSimilarity(new NutchSimilarity());
    writer.addIndexes(dirs);
    writer.close();

    //
    // Put target back
    //
    fs.completeLocalOutput(outputIndex, tmpLocalOutput);

    FileSystem.getNamed("local", conf).delete(localWorkingDir);
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
    Configuration conf = NutchConfiguration.create();
    FileSystem fs = FileSystem.get(conf);
    Path workDir = new Path("indexmerger");
    List indexDirs = new ArrayList();

    int i = 0;
    if ("-workingdir".equals(args[i])) {
      i++;
      workDir = new Path(args[i++]);
    }

    Path outputIndex = new Path(args[i++]);

    for (; i < args.length; i++) {
      indexDirs.addAll(Arrays.asList(fs.listPaths(new Path(args[i]))));
    }

    //
    // Merge the indices
    //
    LOG.info("merging indexes to: " + outputIndex);

    Path[] indexFiles = (Path[])indexDirs.toArray(new Path[indexDirs.size()]);

    FileSystem localFs = FileSystem.getNamed("local", conf);
    if (localFs.exists(workDir)) {
      localFs.delete(workDir);
    }
    localFs.mkdirs(workDir);
    IndexMerger merger =
      new IndexMerger(fs, indexFiles, outputIndex, workDir, conf);
    merger.merge();
    LOG.info("done merging");
    localFs.delete(workDir);
  }
}

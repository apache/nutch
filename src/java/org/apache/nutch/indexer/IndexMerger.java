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

package org.apache.nutch.indexer;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;

import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.LogUtil;
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
public class IndexMerger extends Configured implements Tool {
  public static final Log LOG = LogFactory.getLog(IndexMerger.class);

  public static final String DONE_NAME = "merge.done";

  public IndexMerger() {
    
  }
  
  public IndexMerger(Configuration conf) {
    setConf(conf);
  }
  
  /**
   * Merge all input indexes to the single output index
   */
  public void merge(Path[] indexes, Path outputIndex, Path localWorkingDir) throws IOException {
    LOG.info("merging indexes to: " + outputIndex);

    FileSystem localFs = FileSystem.getLocal(getConf());  
    if (localFs.exists(localWorkingDir)) {
      localFs.delete(localWorkingDir);
    }
    localFs.mkdirs(localWorkingDir);

    // Get local output target
    //
    FileSystem fs = FileSystem.get(getConf());
    if (fs.exists(outputIndex)) {
      throw new FileAlreadyExistsException("Output directory " + outputIndex + " already exists!");
    }

    Path tmpLocalOutput = new Path(localWorkingDir, "merge-output");
    Path localOutput = fs.startLocalOutput(outputIndex, tmpLocalOutput);

    Directory[] dirs = new Directory[indexes.length];
    for (int i = 0; i < indexes.length; i++) {
      if (LOG.isInfoEnabled()) { LOG.info("Adding " + indexes[i]); }
      dirs[i] = new FsDirectory(fs, indexes[i], false, getConf());
    }

    //
    // Merge indices
    //
    IndexWriter writer = new IndexWriter(localOutput.toString(), null, true);
    writer.setMergeFactor(getConf().getInt("indexer.mergeFactor", IndexWriter.DEFAULT_MERGE_FACTOR));
    writer.setMaxBufferedDocs(getConf().getInt("indexer.minMergeDocs", IndexWriter.DEFAULT_MAX_BUFFERED_DOCS));
    writer.setMaxMergeDocs(getConf().getInt("indexer.maxMergeDocs", IndexWriter.DEFAULT_MAX_MERGE_DOCS));
    writer.setTermIndexInterval(getConf().getInt("indexer.termIndexInterval", IndexWriter.DEFAULT_TERM_INDEX_INTERVAL));
    writer.setInfoStream(LogUtil.getDebugStream(LOG));
    writer.setUseCompoundFile(false);
    writer.setSimilarity(new NutchSimilarity());
    writer.addIndexes(dirs);
    writer.close();

    //
    // Put target back
    //
    fs.completeLocalOutput(outputIndex, tmpLocalOutput);
    LOG.info("done merging");
  }

  /** 
   * Create an index for the input files in the named directory. 
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new IndexMerger(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
    String usage = "IndexMerger [-workingdir <workingdir>] outputIndex indexesDir...";
    if (args.length < 2) {
      System.err.println("Usage: " + usage);
      return -1;
    }

    //
    // Parse args, read all index directories to be processed
    //
    FileSystem fs = FileSystem.get(getConf());
    List<Path> indexDirs = new ArrayList<Path>();

    Path workDir = new Path("indexmerger-" + System.currentTimeMillis());  
    int i = 0;
    if ("-workingdir".equals(args[i])) {
      i++;
      workDir = new Path(args[i++], "indexmerger-" + System.currentTimeMillis());
    }

    Path outputIndex = new Path(args[i++]);

    for (; i < args.length; i++) {
      indexDirs.addAll(Arrays.asList(fs.listPaths(new Path(args[i]), HadoopFSUtil.getPassAllFilter())));
    }

    //
    // Merge the indices
    //

    Path[] indexFiles = (Path[])indexDirs.toArray(new Path[indexDirs.size()]);

    try {
      merge(indexFiles, outputIndex, workDir);
      return 0;
    } catch (Exception e) {
      LOG.fatal("IndexMerger: " + StringUtils.stringifyException(e));
      return -1;
    } finally {
      FileSystem.getLocal(getConf()).delete(workDir);
    }
  }
}

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

package org.apache.nutch.searcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.indexer.Indexer;
import org.apache.nutch.util.HadoopFSUtil;

public class LuceneSearchBean implements RPCSearchBean {

  public static final long VERSION = 1L;

  private IndexSearcher searcher;

  private FileSystem fs;

  private Configuration conf;

  /**
   * Construct in a named directory.
   * @param conf
   * @param dir
   * @throws IOException
   */
  public LuceneSearchBean(Configuration conf, Path indexDir, Path indexesDir)
  throws IOException {
    this.conf = conf;
    this.fs = FileSystem.get(this.conf);
    init(indexDir, indexesDir);
  }

  private void init(Path indexDir, Path indexesDir)
  throws IOException {
    if (this.fs.exists(indexDir)) {
      LOG.info("opening merged index in " + indexDir);
      this.searcher = new IndexSearcher(indexDir, this.conf);
    } else {
      LOG.info("opening indexes in " + indexesDir);

      List<Path> vDirs = new ArrayList<Path>();
      FileStatus[] fstats = fs.listStatus(indexesDir, HadoopFSUtil.getPassDirectoriesFilter(fs));
      Path[] directories = HadoopFSUtil.getPaths(fstats);
      for(int i = 0; i < directories.length; i++) {
        Path indexdone = new Path(directories[i], Indexer.DONE_NAME);
        if(fs.isFile(indexdone)) {
          vDirs.add(directories[i]);
        }
      }

      directories = new Path[ vDirs.size() ];
      for(int i = 0; vDirs.size()>0; i++) {
        directories[i] = vDirs.remove(0);
      }

      this.searcher = new IndexSearcher(directories, this.conf);
    }
  }


  @Override
  @Deprecated
  public Hits search(Query query, int numHits, String dedupField,
                     String sortField, boolean reverse)
  throws IOException {
    query.setParams(new QueryParams(numHits, QueryParams.DEFAULT_MAX_HITS_PER_DUP, dedupField, sortField, reverse));
    return searcher.search(query);
  }
  
  @Override
  public Hits search(Query query) throws IOException {
    return searcher.search(query);
  }

  public String getExplanation(Query query, Hit hit) throws IOException {
    return searcher.getExplanation(query, hit);
  }

  public HitDetails getDetails(Hit hit) throws IOException {
    return searcher.getDetails(hit);
  }

  public HitDetails[] getDetails(Hit[] hits) throws IOException {
    return searcher.getDetails(hits);
  }

  public boolean ping() throws IOException {
    return true;
  }

  public void close() throws IOException {
    if (searcher != null) { searcher.close(); }
    if (fs != null) { fs.close(); }
  }

  public long getProtocolVersion(String protocol, long clientVersion)
  throws IOException {
    return VERSION;
  }

}

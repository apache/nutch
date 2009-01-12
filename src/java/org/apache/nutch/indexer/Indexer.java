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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.lucene.LuceneWriter;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/** Create indexes for segments. */
public class Indexer extends Configured implements Tool {

  public static final String DONE_NAME = "index.done";

  public static final Log LOG = LogFactory.getLog(Indexer.class);

  public Indexer() {
    super(null);
  }

  public Indexer(Configuration conf) {
    super(conf);
  }

  public void index(Path luceneDir, Path crawlDb,
                    Path linkDb, List<Path> segments)
  throws IOException {
    LOG.info("Indexer: starting");

    final JobConf job = new NutchJob(getConf());
    job.setJobName("index-lucene " + luceneDir);

    IndexerMapReduce.initMRJob(crawlDb, linkDb, segments, job);

    FileOutputFormat.setOutputPath(job, luceneDir);

    LuceneWriter.addFieldOptions("segment", LuceneWriter.STORE.YES, LuceneWriter.INDEX.NO, job);
    LuceneWriter.addFieldOptions("digest", LuceneWriter.STORE.YES, LuceneWriter.INDEX.NO, job);
    LuceneWriter.addFieldOptions("boost", LuceneWriter.STORE.YES, LuceneWriter.INDEX.NO, job);

    NutchIndexWriterFactory.addClassToConf(job, LuceneWriter.class);

    JobClient.runJob(job);
    LOG.info("Indexer: done");
  }

  public int run(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println("Usage: Indexer <index> <crawldb> <linkdb> <segment> ...");
      return -1;
    }

    final Path luceneDir = new Path(args[0]);
    final Path crawlDb = new Path(args[1]);
    final Path linkDb = new Path(args[2]);

    final List<Path> segments = new ArrayList<Path>();
    for (int i = 3; i < args.length; i++) {
      segments.add(new Path(args[i]));
    }

    try {
      index(luceneDir, crawlDb, linkDb, segments);
      return 0;
    } catch (final Exception e) {
      LOG.fatal("Indexer: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(), new Indexer(), args);
    System.exit(res);
  }
}

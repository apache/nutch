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

package org.apache.nutch.crawl;

import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;
import java.util.logging.*;

import org.apache.nutch.io.*;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;
import org.apache.nutch.mapred.*;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.indexer.DeleteDuplicates;
import org.apache.nutch.indexer.IndexMerger;
import org.apache.nutch.indexer.Indexer;

public class Crawl {
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.crawl.Crawl");

  private static String getDate() {
    return new SimpleDateFormat("yyyyMMddHHmmss").format
      (new Date(System.currentTimeMillis()));
  }

  static {
    NutchConf.get().addConfResource("crawl-tool.xml");
  }

  /* Perform complete crawling and indexing given a set of root urls. */
  public static void main(String args[]) throws Exception {
    if (args.length < 1) {
      System.out.println
        ("Usage: Crawl <urlDir> [-dir d] [-threads n] [-depth i] [-topN N]");
      return;
    }

    JobConf conf = new JobConf(NutchConf.get());
    //conf.addConfResource("crawl-tool.xml");

    File rootUrlDir = null;
    File dir = new File("crawl-" + getDate());
    int threads = conf.getInt("fetcher.threads.fetch", 10);
    int depth = 5;
    int topN = Integer.MAX_VALUE;

    for (int i = 0; i < args.length; i++) {
      if ("-dir".equals(args[i])) {
        dir = new File(args[i+1]);
        i++;
      } else if ("-threads".equals(args[i])) {
        threads = Integer.parseInt(args[i+1]);
        i++;
      } else if ("-depth".equals(args[i])) {
        depth = Integer.parseInt(args[i+1]);
        i++;
      } else if ("-topN".equals(args[i])) {
        topN = Integer.parseInt(args[i+1]);
        i++;
      } else if (args[i] != null) {
        rootUrlDir = new File(args[i]);
      }
    }

    NutchFileSystem fs = NutchFileSystem.get(conf);
    if (fs.exists(dir)) {
      throw new RuntimeException(dir + " already exists.");
    }

    LOG.info("crawl started in: " + dir);
    LOG.info("rootUrlDir = " + rootUrlDir);
    LOG.info("threads = " + threads);
    LOG.info("depth = " + depth);

    if (topN != Integer.MAX_VALUE)
      LOG.info("topN = " + topN);

    File crawlDb = new File(dir + "/crawldb");
    File linkDb = new File(dir + "/linkdb");
    File segments = new File(dir + "/segments");
    File indexes = new File(dir + "/indexes");
    File index = new File(dir + "/index");

    File tmpDir = conf.getLocalFile("crawl", getDate());
      
    // initialize crawlDb
    new Injector(conf).inject(crawlDb, rootUrlDir);
      
    for (int i = 0; i < depth; i++) {             // generate new segment
      File segment =
        new Generator(conf).generate(crawlDb, segments, -1,
                                     topN, System.currentTimeMillis());
      new Fetcher(conf).fetch(segment, threads, Fetcher.isParsing(conf));  // fetch it
      if (!Fetcher.isParsing(conf)) {
        new ParseSegment(conf).parse(segment);    // parse it, if needed
      }
      new CrawlDb(conf).update(crawlDb, segment); // update crawldb
    }
      
    new LinkDb(conf).invert(linkDb, segments); // invert links

    // index, dedup & merge
    new Indexer(conf).index(indexes, crawlDb, linkDb, fs.listFiles(segments));
    new DeleteDuplicates(conf).dedup(new File[] { indexes });
    new IndexMerger(fs, fs.listFiles(indexes), index, tmpDir).merge();

    LOG.info("crawl finished: " + dir);
  }
}

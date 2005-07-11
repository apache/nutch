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
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;
import org.apache.nutch.mapred.*;

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

    File rootUrlFile = null;
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
        rootUrlFile = new File(args[i]);
      }
    }

    NutchFileSystem fs = NutchFileSystem.get(conf);
    if (fs.exists(dir)) {
      throw new RuntimeException(dir + " already exists.");
    }

    LOG.info("crawl started in: " + dir);
    LOG.info("rootUrlFile = " + rootUrlFile);
    LOG.info("threads = " + threads);
    LOG.info("depth = " + depth);

    if (topN != Integer.MAX_VALUE)
      LOG.info("topN = " + depth);

    File crawlDb = new File(dir + "/crawldb");
    File linkDb = new File(dir + "/linkdb");
    File segments = new File(dir + "/segments");
    File index = new File(dir + "/indexes");
      
    // initialize crawlDb
    new Injector(conf).inject(crawlDb, rootUrlFile);
      
    for (int i = 0; i < depth; i++) {             // generate new segment
      File segment =
        new Generator(conf).generate(crawlDb, segments, -1,
                                     topN, System.currentTimeMillis());
      new Fetcher(conf).fetch(segment, threads);  // fetch it
      new ParseSegment(conf).parse(segment);      // parse it
      new CrawlDb(conf).update(crawlDb, segment); // update crawldb
    }
      
    new LinkDb(conf).invert(linkDb, segments); // invert links

    // index
    new Indexer(conf).index(index, linkDb, fs.listFiles(segments));

    LOG.info("crawl finished: " + dir);
  }
}

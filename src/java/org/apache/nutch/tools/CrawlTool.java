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

import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;
import java.util.logging.*;

import org.apache.nutch.io.*;
import org.apache.nutch.db.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;
import org.apache.nutch.fetcher.*;
import org.apache.nutch.indexer.*;

/*
 */
public class CrawlTool {
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.tools.CrawlTool");

  static {
    NutchConf.get().addConfResource("crawl-tool.xml");
  }

  /** Returns a string representing the current date and time that also sorts
   * lexicographically by date. */
  private static String getDate() {
    return new SimpleDateFormat("yyyyMMddHHmmss").format
      (new Date(System.currentTimeMillis()));
  }

  /** Returns the pathname of the latest segment in a segments directory. */
  private static String getLatestSegment(NutchFileSystem nfs, String segmentsDir) throws IOException {
      File bestSegment = null;
      File[] allSegmentFiles = nfs.listFiles(new File(segmentsDir));
      for (int i = 0; i < allSegmentFiles.length; i++) {
          String name = allSegmentFiles[i].getName();
          if (bestSegment == null || bestSegment.getName().compareTo(name) < 0) {
              bestSegment = allSegmentFiles[i];
          }
      }
      return bestSegment.getPath();
  }

  /**
   * Useful in constructing a command-line for other tools
   */
  private static String[] prependFileSystem(String fs, String nameserver, String[] items) {
      String[] results = null;
      if ("-ndfs".equals(fs)) {
          results = new String[items.length + 2];
          results[0] = fs;
          results[1] = nameserver;
          System.arraycopy(items, 0, results, 2, items.length);
      } else if ("-local".equals(fs)) {
          results = new String[items.length + 1];
          results[0] = fs;
          System.arraycopy(items, 0, results, 1, items.length);
      } else {
          results = items;
      }
      return results;
  }

  /* Perform complete crawling and indexing given a set of root urls. */
  public static void main(String args[]) throws Exception {
    if (args.length < 1) {
      System.out.println("Usage: CrawlTool (-local | -ndfs <nameserver:port>) <root_url_file> [-dir d] [-threads n] [-depth i] [-showThreadID]");
      return;
    }

    String fs = "-local";
    String nameserver = "";
    if ("-ndfs".equals(args[0])) {
        fs = "-ndfs";
        nameserver = args[1];
    }
    NutchFileSystem nfs = NutchFileSystem.parseArgs(args, 0);
    try {
        String rootUrlFile = null;
        String dir = new File("crawl-" + getDate()).getCanonicalFile().getName();
        int threads = NutchConf.get().getInt("fetcher.threads.fetch", 10);
        int depth = 5;
        boolean showThreadID = false;

        for (int i = 0; i < args.length; i++) {
            if ("-dir".equals(args[i])) {
                dir = args[i+1];
                i++;
            } else if ("-threads".equals(args[i])) {
                threads = Integer.parseInt(args[i+1]);
                i++;
            } else if ("-depth".equals(args[i])) {
                depth = Integer.parseInt(args[i+1]);
                i++;
            } else if ("-showThreadID".equals(args[i])) {
                showThreadID = true;
            } else if (args[i] != null) {
                rootUrlFile = args[i];
            }
        }

        if (nfs.exists(new File(dir))) {
            throw new RuntimeException(dir + " already exists.");
        }

        LOG.info("crawl started in: " + dir);
        LOG.info("rootUrlFile = " + rootUrlFile);
        LOG.info("threads = " + threads);
        LOG.info("depth = " + depth);

        String db = new File(dir + "/db").getCanonicalPath();
        String segments = new File(dir + "/segments").getCanonicalPath();

        // initialize the web database
        WebDBAdminTool.main(prependFileSystem(fs, nameserver, new String[] { db, "-create"}));
        WebDBInjector.main(prependFileSystem(fs, nameserver, new String[] { db, "-urlfile", rootUrlFile }));

        for (int i = 0; i < depth; i++) {
            // generate a new segment
            FetchListTool.main(prependFileSystem(fs, nameserver, new String[] { db, segments } ));
            String segment = getLatestSegment(nfs, segments);
            Fetcher.main(prependFileSystem(fs, nameserver, new String[] { "-threads", ""+threads, segment } ));
            UpdateDatabaseTool.main(prependFileSystem(fs, nameserver, new String[] { db, segment } ));
        }

        // update segments from db
        UpdateSegmentsFromDb updater =
          new UpdateSegmentsFromDb(nfs, db, segments, dir);
        updater.run();

        // index, dedup & merge
        File workDir = new File(dir, "workdir");
        File[] segmentDirs = nfs.listFiles(new File(segments));
        for (int i = 0; i < segmentDirs.length; i++) {
          IndexSegment.main(prependFileSystem(fs, nameserver, new String[] { segmentDirs[i].toString(), "-dir", workDir.getPath() } ));
        }
        DeleteDuplicates.main(prependFileSystem(fs, nameserver, new String[] { segments }));

        LOG.info("Merging segment indexes... ");
        IndexMerger merger = new IndexMerger(nfs, segmentDirs, new File(dir + "/index"), workDir);
        merger.merge();

        FileUtil.fullyDelete(workDir);

        LOG.info("crawl finished: " + dir);
    } finally {
        nfs.close();
    }
  }
}

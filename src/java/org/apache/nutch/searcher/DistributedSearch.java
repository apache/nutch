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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.nutch.util.NutchConfiguration;

/** Search/summary servers. */
public class DistributedSearch {

  private DistributedSearch() {}                  // no public ctor

  /** Runs a search/summary server. */
  public static class Server {
    public static void main(String[] args) throws Exception {
      final String usage = "DistributedSearch$Server <port> <crawl dir>";

      if (args.length == 0 || args.length > 2) {
        System.err.println(usage);
        System.exit(-1);
      }

      final int port = Integer.parseInt(args[0]);
      final Path directory = new Path(args[1]);

      final Configuration conf = NutchConfiguration.create();

      final org.apache.hadoop.ipc.Server server =
        getServer(conf, directory, port);
      server.start();
      server.join();
    }

    static org.apache.hadoop.ipc.Server getServer(Configuration conf,
        Path directory, int port) throws IOException{
      final NutchBean bean = new NutchBean(conf, directory);
      final int numHandlers = conf.getInt("searcher.num.handlers", 10);
      return RPC.getServer(bean, "0.0.0.0", port, numHandlers, true, conf);
    }

  }

  public static class IndexServer {
    /** Runs a lucene search server. */
    public static void main(String[] args) throws Exception {
      final String usage = "DistributedSearch$IndexServer <port> <crawl dir>";
      if (args.length == 0 || args.length > 2) {
        System.err.println(usage);
        System.exit(-1);
      }

      final int port = Integer.parseInt(args[0]);
      final Path dir = new Path(args[1]);

      final Configuration conf = NutchConfiguration.create();

      final LuceneSearchBean bean = new LuceneSearchBean(conf,
          new Path(dir, "index"), new Path(dir, "indexes"));
      final org.apache.hadoop.ipc.RPC.Server server =
        RPC.getServer(bean, "0.0.0.0", port, 10, false, conf);
      server.start();
      server.join();
    }
  }

  public static class SegmentServer {
    /** Runs a summary server. */
    public static void main(String[] args) throws Exception {
      final String usage =
        "DistributedSearch$SegmentServer <port> <crawl dir>";
      if (args.length < 2) {
        System.err.println(usage);
        System.exit(1);
      }

      final Configuration conf = NutchConfiguration.create();
      final int port = Integer.parseInt(args[0]);
      final Path segmentsDir = new Path(args[1], "segments");

      final FetchedSegments segments = new FetchedSegments(conf, segmentsDir);

      final org.apache.hadoop.ipc.RPC.Server server =
        RPC.getServer(segments, "0.0.0.0", port, conf);

      server.start();
      server.join();
    }
  }
}

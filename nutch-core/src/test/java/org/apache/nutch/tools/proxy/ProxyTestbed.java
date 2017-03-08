package org.apache.nutch.tools.proxy;

/*
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

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.proxy.AsyncProxyServlet;

public class ProxyTestbed {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err
          .println("TestbedProxy [-seg <segment_name> | -segdir <segments>] [-port <nnn>] [-forward] [-fake] [-delay nnn] [-debug]");
      System.err
          .println("-seg <segment_name>\tpath to a single segment (can be specified multiple times)");
      System.err
          .println("-segdir <segments>\tpath to a parent directory of multiple segments (as above)");
      System.err
          .println("-port <nnn>\trun the proxy on port <nnn> (special permissions may be needed for ports < 1024)");
      System.err
          .println("-forward\tif specified, requests to all unknown urls will be passed to");
      System.err
          .println("\t\toriginal servers. If false (default) unknown urls generate 404 Not Found.");
      System.err
          .println("-delay\tdelay every response by nnn seconds. If delay is negative use a random value up to nnn");
      System.err
          .println("-fake\tif specified, requests to all unknown urls will succeed with fake content");
      throw new IllegalArgumentException();
    }

    Configuration conf = NutchConfiguration.create();
    int port = conf.getInt("segment.proxy.port", 8181);
    boolean forward = false;
    boolean fake = false;
    boolean delay = false;
    boolean debug = false;
    int delayVal = 0;

    HashSet<Path> segs = new HashSet<Path>();
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-segdir")) {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fstats = fs.listStatus(new Path(args[++i]));
        Path[] paths = HadoopFSUtil.getPaths(fstats);
        segs.addAll(Arrays.asList(paths));
      } else if (args[i].equals("-port")) {
        port = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-forward")) {
        forward = true;
      } else if (args[i].equals("-delay")) {
        delay = true;
        delayVal = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-fake")) {
        fake = true;
      } else if (args[i].equals("-debug")) {
        debug = true;
      } else if (args[i].equals("-seg")) {
        segs.add(new Path(args[++i]));
      } else {
        LOG.error("Unknown argument: " + args[i]);
        throw new IllegalArgumentException("Unknown argument: " + args[i]);
      }
    }

    // Create the server
    Server server = new Server();
    SocketConnector connector = new SocketConnector();
    connector.setPort(port);
    connector.setResolveNames(false);
    server.addConnector(connector);

    // create a list of handlers
    HandlerList list = new HandlerList();
    server.addHandler(list);

    if (debug) {
      LOG.info("* Added debug handler.");
      list.addHandler(new LogDebugHandler());
    }

    if (delay) {
      LOG.info("* Added delay handler: "
          + (delayVal < 0 ? "random delay up to " + (-delayVal)
              : "constant delay of " + delayVal));
      list.addHandler(new DelayHandler(delayVal));
    }

    // XXX alternatively, we can add the DispatchHandler as the first one,
    // XXX to activate handler plugins and redirect requests to appropriate
    // XXX handlers ... Here we always load these handlers

    Iterator<Path> it = segs.iterator();
    while (it.hasNext()) {
      Path p = it.next();
      try {
        SegmentHandler segment = new SegmentHandler(conf, p);
        list.addHandler(segment);
        LOG.info("* Added segment handler for: " + p);
      } catch (Exception e) {
        LOG.warn("Skipping segment '" + p + "': "
            + StringUtils.stringifyException(e));
      }
    }
    if (forward) {
      LOG.info("* Adding forwarding proxy for all unknown urls ...");
      ServletHandler servlets = new ServletHandler();
      servlets.addServletWithMapping(AsyncProxyServlet.class, "/*");
      servlets.addFilterWithMapping(LogDebugHandler.class, "/*", Handler.ALL);
      list.addHandler(servlets);
    }
    if (fake) {
      LOG.info("* Added fake handler for remaining URLs.");
      list.addHandler(new FakeHandler());
    }
    list.addHandler(new NotFoundHandler());
    // Start the http server
    server.start();
    server.join();
  }
}

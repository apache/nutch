/*******************************************************************************
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
 ******************************************************************************/
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.tools.proxy.FakeHandler.Mode;
import org.apache.nutch.util.NutchConfiguration;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.proxy.AsyncProxyServlet;

public class TestbedProxy {
  private static final Logger LOG = LoggerFactory.getLogger(TestbedProxy.class);

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("TestbedProxy [-port <nnn>] [-forward] [-fake [...]] [-delay nnn] [-debug]");
      System.err.println("-port <nnn>\trun the proxy on port <nnn> (special permissions may be needed for ports < 1024)");
      System.err.println("-forward\tif specified, requests to all unknown urls will be passed to");
      System.err.println("\t\toriginal servers. If false (default) unknown urls generate 404 Not Found.");
      System.err.println("-delay\tdelay every response by nnn seconds. If delay is negative use a random value up to nnn");
      System.err.println("-fake\tif specified, requests to all unknown urls will succeed with fake content");
      System.err.println("\nAdditional options for -fake handler (all optional):");
      System.err.println("\t-hostMode (u | r)\tcreate unique host names, or pick random from a pool");
      System.err.println("\t-pageMode (u | r)\tcreate unique page names, or pick random from a pool");
      System.err.println("\t-numHosts N\ttotal number of hosts when using hostMode r");
      System.err.println("\t-numPages N\ttotal number of pages per host when using pageMode r");
      System.err.println("\t-intLinks N\tnumber of internal (same host) links per page");
      System.err.println("\t-extLinks N\tnumber of external (other host) links per page");
      System.err.println("\nDefaults for -fake handler:");
      System.err.println("\t-hostMode r");
      System.err.println("\t-pageMode r");
      System.err.println("\t-numHosts 1000000");
      System.err.println("\t-numPages 10000");
      System.err.println("\t-intLinks 10");
      System.err.println("\t-extLinks 5");
      System.exit(-1);
    }
    
    Configuration conf = NutchConfiguration.create();
    int port = conf.getInt("batch.proxy.port", 8181);
    boolean forward = false;
    boolean fake = false;
    boolean delay = false;
    boolean debug = false;
    int delayVal = 0;
    Mode pageMode = Mode.RANDOM;
    Mode hostMode = Mode.RANDOM;
    int numHosts = 1000000;
    int numPages = 10000;
    int intLinks = 10;
    int extLinks = 5;
    
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-port")) {
        port = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-forward")) {
        forward = true;
      } else if (args[i].equals("-delay")) {
        delay = true;
        delayVal = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-fake")) {
        fake = true;
      } else if (args[i].equals("-hostMode")) {
        if (args[++i].equals("u")) {
          hostMode = Mode.UNIQUE;
        }
      } else if (args[i].equals("-pageMode")) {
        if (args[++i].equals("u")) {
          pageMode = Mode.UNIQUE;
        }
      } else if (args[i].equals("-numHosts")) {
        numHosts = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-numPages")) {
        numPages = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-intLinks")) {
        intLinks = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-extLinks")) {
        extLinks = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-debug")) {
        debug = true;
      } else {
        LOG.error("Unknown argument: " + args[i]);
        System.exit(-1);
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
      LOG.info("* Added delay handler: " + (delayVal < 0 ? "random delay up to " + (-delayVal) : "constant delay of " + delayVal));
      list.addHandler(new DelayHandler(delayVal));
    }
    
    // XXX alternatively, we can add the DispatchHandler as the first one,
    // XXX to activate handler plugins and redirect requests to appropriate
    // XXX handlers ... Here we always load these handlers

    if (forward) {
      LOG.info("* Adding forwarding proxy for all unknown urls ...");
      ServletHandler servlets = new ServletHandler();
      servlets.addServletWithMapping(AsyncProxyServlet.class, "/*");
      servlets.addFilterWithMapping(LogDebugHandler.class, "/*", Handler.ALL);
      list.addHandler(servlets);
    }
    if (fake) {
      LOG.info("* Added fake handler for remaining URLs.");
      list.addHandler(new FakeHandler(hostMode, pageMode, intLinks, extLinks,
          numHosts, numPages));
    }
    list.addHandler(new NotFoundHandler());
    // Start the http server
    server.start();
    server.join();
  }
}

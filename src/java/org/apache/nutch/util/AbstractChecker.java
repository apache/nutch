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

package org.apache.nutch.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scaffolding class for the various Checker implementations. Can process cmdline input, stdin and TCP connections.
 * 
 * @author Jurian Broertjes
 */
public abstract class AbstractChecker extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected boolean keepClientCnxOpen = false;
  protected int tcpPort = -1;
  protected boolean stdin = true;
  protected String usage;

  // Actual function for the processing of a single input
  protected abstract int process(String line, StringBuilder output) throws Exception;

  protected int parseArgs(String[] args, int i) {
    if (args[i].equals("-listen")) {
      tcpPort = Integer.parseInt(args[++i]);
      return 2;
    } else if (args[i].equals("-keepClientCnxOpen")) {
      keepClientCnxOpen = true;
      return 1;
    } else if (args[i].equals("-stdin")) {
      stdin = true;
      return 1;
    }
    return 0;
  }

  protected int run() throws Exception {
    // In listening mode?
    if (tcpPort != -1) {
      processTCP(tcpPort);
      return 0;
    } else if (stdin) {
      return processStdin();
    }
    // Nothing to do?
    return -1;
  }

  // Process single input and return
  protected int processSingle(String input) throws Exception {
    StringBuilder output = new StringBuilder();
    int ret = process(input, output);
    System.out.println(output);
    return ret;
  }

  // Read from stdin
  protected int processStdin() throws Exception {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;
    while ((line = in.readLine()) != null) {
      StringBuilder output = new StringBuilder();
      int ret = process(line, output);
      System.out.println(output);
    }
    return 0;
  }

  // Open TCP socket and process input
  protected void processTCP(int tcpPort) throws Exception {
    ServerSocket server = null;

    try {
      server = new ServerSocket();
      server.bind(new InetSocketAddress(tcpPort));
      LOG.info(server.toString());
    } catch (Exception e) {
      LOG.error("Could not listen on port " + tcpPort, e);
      System.exit(-1);
    }
    
    while(true){
      Worker worker;
      try {
        worker = new Worker(server.accept());
        Thread thread = new Thread(worker);
        thread.start();
      } catch (Exception e) {
        LOG.error("Accept failed: " + tcpPort, e);
        System.exit(-1);
      }
    }
  }

  private class Worker implements Runnable {
    private Socket client;

    Worker(Socket client) {
      this.client = client;
      LOG.info(client.toString());
    }

    public void run() {
      // Setup streams
      BufferedReader in = null;
      OutputStream out = null;
      try {
        in = new BufferedReader(new InputStreamReader(client.getInputStream()));
        out = client.getOutputStream();
      } catch (IOException e) {
        LOG.error("Failed initializing streams: ", e);
        return;
      }

      // Listen for work
      if (keepClientCnxOpen) {
        try {
          while (readWrite(in, out)) {} // keep connection open until it closes
        } catch(Exception e) {
          LOG.error("Read/Write failed: ", e);
        }
      } else {
        try {
          readWrite(in, out);
        } catch(Exception e) {
          LOG.error("Read/Write failed: ", e);
        }
      }

      try { // close ourselves
        client.close();
      } catch (Exception e){
        LOG.error(e.toString());
      }
    }
    
    protected boolean readWrite(BufferedReader in, OutputStream out) throws Exception {
      String line = in.readLine();

      if (line == null) {
        // End of stream
        return false;
      }

      if (line.trim().length() > 1) {
        // The actual work
        StringBuilder output = new StringBuilder();
        process(line, output);
        output.append("\n");
        out.write(output.toString().getBytes(StandardCharsets.UTF_8));
      }
      return true;
    }
  }

  protected ProtocolOutput getProtocolOutput(String url, CrawlDatum datum) throws Exception {
    ProtocolFactory factory = new ProtocolFactory(getConf());
    Protocol protocol = factory.getProtocol(url);
    Text turl = new Text(url);
    return protocol.getProtocolOutput(turl, datum);
  }

}
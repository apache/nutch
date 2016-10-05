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

package org.apache.nutch.net;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.util.NutchConfiguration;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks one given filter or all filters.
 * 
 * @author John Xing
 */
public class URLFilterChecker {

  private Configuration conf;
  private static String filterName = null;
  protected static boolean keepClientCnxOpen = false;
  protected static int tcpPort = -1;
  protected URLFilters filters = null;
  
  public static final Logger LOG = LoggerFactory
      .getLogger(URLFilterChecker.class);
  
  public URLFilterChecker(Configuration conf) {
    System.out.println("Checking combination of all URLFilters available");
    this.conf = conf;
    if (filterName != null) {
        this.conf.set("plugin.includes", filterName);
    }
    filters = new URLFilters(this.conf);
  }
  
  public void run() throws Exception {
    // In listening mode?
    if (tcpPort == -1) {
      // No, just fetch and display
      checkStdin();
    } else {
      // Listen on socket and start workers on incoming requests
      listen();
    }
  }
  
  private void listen() throws Exception {
    ServerSocket server = null;

    try{
      server = new ServerSocket();
      server.bind(new InetSocketAddress(tcpPort));
      LOG.info(server.toString());
    } catch (Exception e) {
      LOG.error("Could not listen on port " + tcpPort);
      System.exit(-1);
    }
    
    while(true){
      Worker worker;
      try{
        worker = new Worker(server.accept());
        Thread thread = new Thread(worker);
        thread.start();
      } catch (Exception e) {
        LOG.error("Accept failed: " + tcpPort);
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
      if (keepClientCnxOpen) {
        while (true) { // keep connection open until closes
          readWrite();
        }
      } else {
        readWrite();
        
        try { // close ourselves
          client.close();
        } catch (Exception e){
          LOG.error(e.toString());
        }
      }
    }
    
    protected void readWrite() {
      String line;
      BufferedReader in = null;
      PrintWriter out = null;
      
      try{
        in = new BufferedReader(new InputStreamReader(client.getInputStream()));
      } catch (Exception e) {
        LOG.error("in or out failed");
        System.exit(-1);
      }

      try{
        line = in.readLine();        

        String result = filters.filter(line);
        String output;
        if (result != null) {
          output = "+" + result + "\n";
        } else {
          output = "-" + line + "\n";;
        }        
        
        client.getOutputStream().write(output.getBytes(Charset.forName("UTF-8")));
      }catch (Exception e) {
        LOG.error("Read/Write failed: " + e);
      }
    }
  }

  private void checkStdin() throws Exception {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;
    
    while ((line = in.readLine()) != null) {
      String out = filters.filter(line);
      if (out != null) {
        System.out.print("+");
        System.out.println(out);
      } else {
        System.out.print("-");
        System.out.println(line);
      }
    }
  }

  public static void main(String[] args) throws Exception {

    String usage = "Usage: URLFilterChecker (-filterName filterName | -allCombined) [-listen <port>] [-keepClientCnxOpen]) \n"
        + "Tool takes a list of URLs, one per line, passed via STDIN.\n";

    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-filterName")) {
        filterName = args[++i];
      } else if (args[i].equals("-listen")) {
        tcpPort = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-keepClientCnxOpen")) {
        keepClientCnxOpen = true;
      }
    }
    
    URLFilterChecker checker = new URLFilterChecker(NutchConfiguration.create());
    checker.run();
    System.exit(0);
  }
}

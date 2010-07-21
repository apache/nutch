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
package org.apache.nutch.util;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.DefaultHandler;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.handler.ResourceHandler;
import org.mortbay.jetty.servlet.Context;

import com.sun.net.httpserver.HttpContext;

public class CrawlTestUtil {

  private static final Log LOG = LogFactory.getLog(CrawlTestUtil.class);

  /**
   * For now we need to manually construct our Configuration, because we need to
   * override the default one and it is currently not possible to use
   * dynamically set values.
   * 
   * @return
   * @deprecated Use {@link #createConfiguration()} instead
   */
  public static Configuration create() {
    return createConfiguration();
  }

  /**
   * For now we need to manually construct our Configuration, because we need to
   * override the default one and it is currently not possible to use
   * dynamically set values.
   * 
   * @return
   */
  public static Configuration createConfiguration() {
    Configuration conf = new Configuration();
    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site.xml");
    conf.addResource("crawl-tests.xml");
    return conf;
  }

  /**
   * Generate seedlist
   * 
   * @see TestInjector
   * @throws IOException
   */
  public static void generateSeedList(FileSystem fs, Path urlPath,
      List<String> contents) throws IOException {
    FSDataOutputStream out;
    Path file = new Path(urlPath, "urls.txt");
    fs.mkdirs(urlPath);
    out = fs.create(file);
    Iterator<String> iterator = contents.iterator();
    while (iterator.hasNext()) {
      String url = iterator.next();
      out.writeBytes(url);
      out.writeBytes("\n");
    }
    out.flush();
    out.close();
  }

  /**
   * Creates a new JettyServer with one static root context
   * 
   * @param port
   *          port to listen to
   * @param staticContent
   *          folder where static content lives
   * @throws UnknownHostException
   */
  public static Server getServer(int port, String staticContent)
      throws UnknownHostException {
    Server webServer = new org.mortbay.jetty.Server(port);
    ResourceHandler handler = new ResourceHandler();
    handler.setResourceBase(staticContent);
    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[]{handler, new DefaultHandler()});
    webServer.setHandler(handlers);
    return webServer;
  }
}

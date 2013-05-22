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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.crawl.URLWebPage;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.DefaultHandler;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.handler.ResourceHandler;

public class CrawlTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CrawlTestUtil.class);

  /**
   * For now we need to manually construct our Configuration, because we need to
   * override the default one and it is currently not possible to use
   * dynamically set values.
   * 
   * @return
   * @deprecated Use {@link #createConfiguration()} instead
   */
  @Deprecated
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
   * Read entries from a data store
   *
   * @return list of matching {@link URLWebPage} objects
   * @throws Exception
   */
  public static ArrayList<URLWebPage> readContents(DataStore<String,WebPage> store,
      Mark requiredMark, String... fields) throws Exception {
    ArrayList<URLWebPage> l = new ArrayList<URLWebPage>();

    Query<String, WebPage> query = store.newQuery();
    if (fields != null) {
      query.setFields(fields);
    }

    Result<String, WebPage> results = store.execute(query);
    while (results.next()) {
      try {
        WebPage page = results.get();
        String url = results.getKey();

        if (page == null)
          continue;

        if (requiredMark != null && requiredMark.checkMark(page) == null)
          continue;

        l.add(new URLWebPage(TableUtil.unreverseUrl(url), (WebPage)page.clone()));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return l;
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

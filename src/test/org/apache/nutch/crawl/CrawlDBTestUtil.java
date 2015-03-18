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
package org.apache.nutch.crawl;

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
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.ContextHandler;
import org.mortbay.jetty.handler.ResourceHandler;

public class CrawlDBTestUtil {

  private static final Logger LOG = LoggerFactory
      .getLogger(CrawlDBTestUtil.class);

  /**
   * Creates synthetic crawldb
   * 
   * @param fs
   *          filesystem where db will be created
   * @param crawldb
   *          path were db will be created
   * @param init
   *          urls to be inserted, objects are of type URLCrawlDatum
   * @throws Exception
   */
  public static void createCrawlDb(Configuration conf, FileSystem fs,
      Path crawldb, List<URLCrawlDatum> init) throws Exception {
    LOG.trace("* creating crawldb: " + crawldb);
    Path dir = new Path(crawldb, CrawlDb.CURRENT_NAME);
    MapFile.Writer writer = new MapFile.Writer(conf, fs, new Path(dir,
        "part-00000").toString(), Text.class, CrawlDatum.class);
    Iterator<URLCrawlDatum> it = init.iterator();
    while (it.hasNext()) {
      URLCrawlDatum row = it.next();
      LOG.info("adding:" + row.url.toString());
      writer.append(new Text(row.url), row.datum);
    }
    writer.close();
  }

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
    conf.addResource("crawl-tests.xml");
    return conf;
  }

  public static class URLCrawlDatum {

    public Text url;

    public CrawlDatum datum;

    public URLCrawlDatum(Text url, CrawlDatum datum) {
      this.url = url;
      this.datum = datum;
    }
  }

  /**
   * Generate seedlist
   * 
   * @throws IOException
   */
  public static void generateSeedList(FileSystem fs, Path urlPath,
      List<String> urls) throws IOException {
    generateSeedList(fs, urlPath, urls, new ArrayList<String>());
  }

  /**
   * Generate seedlist
   * 
   * @throws IOException
   */
  public static void generateSeedList(FileSystem fs, Path urlPath,
      List<String> urls, List<String> metadata) throws IOException {
    FSDataOutputStream out;
    Path file = new Path(urlPath, "urls.txt");
    fs.mkdirs(urlPath);
    out = fs.create(file);

    Iterator<String> urls_i = urls.iterator();
    Iterator<String> metadata_i = metadata.iterator();

    String url;
    String md;
    while (urls_i.hasNext()) {
      url = urls_i.next();

      out.writeBytes(url);

      if (metadata_i.hasNext()) {
        md = metadata_i.next();
        out.writeBytes(md);
      }

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
    Server webServer = new org.mortbay.jetty.Server();
    SocketConnector listener = new SocketConnector();
    listener.setPort(port);
    listener.setHost("127.0.0.1");
    webServer.addConnector(listener);
    ContextHandler staticContext = new ContextHandler();
    staticContext.setContextPath("/");
    staticContext.setResourceBase(staticContent);
    staticContext.addHandler(new ResourceHandler());
    webServer.addHandler(staticContext);
    return webServer;
  }
}

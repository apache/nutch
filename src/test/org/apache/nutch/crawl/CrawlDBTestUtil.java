/*
 * Copyright 2006 The Apache Software Foundation
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

import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.UTF8;

public class CrawlDBTestUtil {

  private static final Log LOG = LogFactory.getLog(CrawlDBTestUtil.class);

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
  public static void createCrawlDb(FileSystem fs, Path crawldb, List<URLCrawlDatum> init)
      throws Exception {
    LOG.trace("* creating crawldb: " + crawldb);
    Path dir = new Path(crawldb, CrawlDatum.DB_DIR_NAME);
    MapFile.Writer writer = new MapFile.Writer(fs, new Path(dir, "part-00000")
        .toString(), UTF8.class, CrawlDatum.class);
    Iterator<URLCrawlDatum> it = init.iterator();
    while (it.hasNext()) {
      URLCrawlDatum row = it.next();
      LOG.info("adding:" + row.url.toString());
      writer.append(new UTF8(row.url), row.datum);
    }
    writer.close();
  }

  /**
   * For now we need to manually construct our Configuration, because we need to
   * override the default one and it is currently not possible to use dynamically
   * set values.
   * 
   * @return
   */
  public static Configuration create(){
    Configuration conf=new Configuration();
    conf.addDefaultResource("nutch-default.xml");
    conf.addFinalResource("crawl-tests.xml");
    return conf;
  }

  public static class URLCrawlDatum {

    UTF8 url;

    CrawlDatum datum;

    public URLCrawlDatum(UTF8 url, CrawlDatum datum) {
      this.url = url;
      this.datum = datum;
    }
  }
}

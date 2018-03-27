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

package org.apache.nutch.crawl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;

/**
 * This class provides a way to separate the URL normalization and filtering
 * steps from the rest of CrawlDb manipulation code.
 * 
 * @author Andrzej Bialecki
 */
public class CrawlDbFilter extends
    Mapper<Text, CrawlDatum, Text, CrawlDatum> {
  public static final String URL_FILTERING = "crawldb.url.filters";
  public static final String URL_NORMALIZING = "crawldb.url.normalizers";
  public static final String URL_NORMALIZING_SCOPE = "crawldb.url.normalizers.scope";

  private boolean urlFiltering;
  private boolean urlNormalizers;

  private boolean url404Purging;
  private boolean purgeOrphans;
  private URLFilters filters;
  private URLNormalizers normalizers;

  private String scope;

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());


  public void setup(Mapper<Text, CrawlDatum, Text, CrawlDatum>.Context context) {
    Configuration conf = context.getConfiguration();
    urlFiltering = conf.getBoolean(URL_FILTERING, false);
    urlNormalizers = conf.getBoolean(URL_NORMALIZING, false);
    url404Purging = conf.getBoolean(CrawlDb.CRAWLDB_PURGE_404, false);
    purgeOrphans = conf.getBoolean(CrawlDb.CRAWLDB_PURGE_ORPHANS, false);

    if (urlFiltering) {
      filters = new URLFilters(conf);
    }
    if (urlNormalizers) {
      scope = conf.get(URL_NORMALIZING_SCOPE, URLNormalizers.SCOPE_CRAWLDB);
      normalizers = new URLNormalizers(conf, scope);
    }
  }

  public void close() {
  }

  private Text newKey = new Text();

  public void map(Text key, CrawlDatum value,
      Context context) throws IOException, InterruptedException {

    String url = key.toString();

    // https://issues.apache.org/jira/browse/NUTCH-1101 check status first,
    // cheaper than normalizing or filtering
    if (url404Purging && CrawlDatum.STATUS_DB_GONE == value.getStatus()) {
      context.getCounter("CrawlDB filter",
        "Gone records removed").increment(1);
      return;
    }
    // Whether to remove orphaned pages
    // https://issues.apache.org/jira/browse/NUTCH-1932
    if (purgeOrphans && CrawlDatum.STATUS_DB_ORPHAN == value.getStatus()) {
      context.getCounter("CrawlDB filter",
        "Orphan records removed").increment(1);
      return;
    }
    if (url != null && urlNormalizers) {
      try {
        url = normalizers.normalize(url, scope); // normalize the url
      } catch (Exception e) {
        LOG.warn("Skipping " + url + ":" + e);
        url = null;
      }
    }
    if (url != null && urlFiltering) {
      try {
        url = filters.filter(url); // filter the url
      } catch (Exception e) {
        LOG.warn("Skipping " + url + ":" + e);
        url = null;
      }
    }
    if (url == null) {
      context.getCounter("CrawlDB filter", "URLs filtered").increment(1);
    } else {
      // URL has passed filters
      newKey.set(url); // collect it
      context.write(newKey, value);
    }
  }
}

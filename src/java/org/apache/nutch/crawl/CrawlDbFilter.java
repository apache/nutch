/**
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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.UrlNormalizer;
import org.apache.nutch.net.UrlNormalizerFactory;

/**
 * This class provides a way to separate the URL normalization
 * and filtering steps from the rest of CrawlDb manipulation code.
 * 
 * @author Andrzej Bialecki
 */
public class CrawlDbFilter implements Mapper {
  public static final String URL_FILTERING = "crawldb.url.filters";

  public static final String URL_NORMALIZING = "crawldb.url.normalizer";

  private boolean urlFiltering;

  private boolean urlNormalizer;

  private URLFilters filters;

  private UrlNormalizer normalizer;

  private JobConf jobConf;

  public static final Log LOG = LogFactory.getLog(CrawlDbFilter.class);

  public void configure(JobConf job) {
    this.jobConf = job;
    urlFiltering = job.getBoolean(URL_FILTERING, false);
    urlNormalizer = job.getBoolean(URL_NORMALIZING, false);
    if (urlFiltering) {
      filters = new URLFilters(job);
    }
    if (urlNormalizer) {
      normalizer = new UrlNormalizerFactory(job).getNormalizer();
    }
  }

  public void close() {}

  public void map(WritableComparable key, Writable value, OutputCollector output, Reporter reporter) throws IOException {

    String url = key.toString();
    if (urlNormalizer) {
      try {
        url = normalizer.normalize(url); // normalize the url
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
    if (url != null) { // if it passes
      UTF8 newKey = (UTF8) key;
      newKey.set(url); // collect it
      output.collect(newKey, value);
    }
  }
}

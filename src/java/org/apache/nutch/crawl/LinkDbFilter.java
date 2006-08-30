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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 * and filtering steps from the rest of LinkDb manipulation code.
 * 
 * @author Andrzej Bialecki
 */
public class LinkDbFilter implements Mapper {
  public static final String URL_FILTERING = "linkdb.url.filters";

  public static final String URL_NORMALIZING = "linkdb.url.normalizer";

  private boolean filter;

  private boolean normalize;

  private URLFilters filters;

  private UrlNormalizer normalizer;

  private JobConf jobConf;
  
  public static final Log LOG = LogFactory.getLog(LinkDbFilter.class);
  
  public void configure(JobConf job) {
    this.jobConf = job;
    filter = job.getBoolean(URL_FILTERING, false);
    normalize = job.getBoolean(URL_NORMALIZING, false);
    if (filter) {
      filters = new URLFilters(job);
    }
    if (normalize) {
      normalizer = new UrlNormalizerFactory(job).getNormalizer();
    }
  }

  public void close() {}

  public void map(WritableComparable key, Writable value, OutputCollector output, Reporter reporter) throws IOException {
    String url = key.toString();
    if (normalize) {
      try {
        url = normalizer.normalize(url); // normalize the url
      } catch (Exception e) {
        LOG.warn("Skipping " + url + ":" + e);
        url = null;
      }
    }
    if (url != null && filter) {
      try {
        url = filters.filter(url); // filter the url
      } catch (Exception e) {
        LOG.warn("Skipping " + url + ":" + e);
        url = null;
      }
    }
    if (url == null) return; // didn't pass the filters
    Inlinks inlinks = (Inlinks)value;
    Iterator it = inlinks.iterator();
    String fromUrl = null;
    while (it.hasNext()) {
      Inlink inlink = (Inlink)it.next();
      fromUrl = inlink.getFromUrl();
      if (normalize) {
        try {
          fromUrl = normalizer.normalize(fromUrl); // normalize the url
        } catch (Exception e) {
          LOG.warn("Skipping " + fromUrl + ":" + e);
          fromUrl = null;
        }
      }
      if (fromUrl != null && filter) {
        try {
          fromUrl = filters.filter(fromUrl); // filter the url
        } catch (Exception e) {
          LOG.warn("Skipping " + fromUrl + ":" + e);
          fromUrl = null;
        }
      }
      if (fromUrl == null) { // should be discarded
        it.remove();
      }
    }
    if (inlinks.size() == 0) return; // don't collect empy inlinks
    output.collect(key, inlinks);
  }
}

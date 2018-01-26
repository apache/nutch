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

package org.apache.nutch.indexer.urlmeta;

import java.lang.invoke.MethodHandles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;

/**
 * This is part of the URL Meta plugin. It is designed to enhance the NUTCH-655
 * patch, by doing two things: 1. Meta Tags that are supplied with your Crawl
 * URLs, during injection, will be propagated throughout the outlinks of those
 * Crawl URLs. 2. When you index your URLs, the meta tags that you specified
 * with your URLs will be indexed alongside those URLs--and can be directly
 * queried, assuming you have done everything else correctly.
 * 
 * The flat-file of URLs you are injecting should, per NUTCH-655, be
 * tab-delimited in the form of:
 * 
 * [www.url.com]\t[key1]=[value1]\t[key2]=[value2]...[keyN]=[valueN]
 * 
 * Be aware that if you collide with keywords that are already in use (such as
 * nutch.score/nutch.fetchInterval) then you are in for some unpredictable
 * behavior.
 * 
 * Furthermore, in your nutch-site.xml config, you must specify that this plugin
 * is to be used (1), as well as what (2) Meta Tags it should actively look for.
 * This does not mean that you must use these tags for every URL, but it does
 * mean that you must list _all_ of meta tags that you have specified. If you
 * want them to be propagated and indexed, that is.
 * 
 * 1. As of Nutch 1.2, the property "plugin.includes" looks as follows:
 * &lt;value&gt;protocol-http|urlfilter-regex|parse-(text|html|js|tika|rss)|index
 * -(basic|anchor)|query-(basic|site|url)|response-(json|xml)|summary-basic
 * |scoring-opic|urlnormalizer-(pass|regex|basic)&lt;/value&gt; You must change
 * "index-(basic|anchor)" to "index-(basic|anchor|urlmeta)", in order to call
 * this plugin.
 * 
 * 2. You must also specify the property "urlmeta.tags", who's values are
 * comma-delimited &lt;value&gt;key1, key2, key3&lt;/value&gt;
 * 
 * TODO: It may be ideal to offer two separate properties, to specify what gets
 * indexed versus merely propagated.
 * 
 */
public class URLMetaIndexingFilter implements IndexingFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  private static final String CONF_PROPERTY = "urlmeta.tags";
  private static String[] urlMetaTags;
  private Configuration conf;

  /**
   * This will take the metatags that you have listed in your "urlmeta.tags"
   * property, and looks for them inside the CrawlDatum object. If they exist,
   * this will add it as an attribute inside the NutchDocument.
   * 
   * @see IndexingFilter#filter
   */
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {
    if (conf != null)
      this.setConf(conf);

    if (urlMetaTags == null || doc == null)
      return doc;

    for (String metatag : urlMetaTags) {
      Text metadata = (Text) datum.getMetaData().get(new Text(metatag));

      if (metadata != null)
        doc.add(metatag, metadata.toString());
    }

    return doc;
  }

  /** Boilerplate */
  public Configuration getConf() {
    return conf;
  }

  /**
   * handles conf assignment and pulls the value assignment from the
   * "urlmeta.tags" property
   */
  public void setConf(Configuration conf) {
    this.conf = conf;

    if (conf == null)
      return;

    urlMetaTags = conf.getStrings(CONF_PROPERTY);
  }
}

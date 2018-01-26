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

package org.apache.nutch.indexer.tld;

import java.lang.invoke.MethodHandles;
import java.net.URL;

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
import org.apache.nutch.util.URLUtil;
import org.apache.nutch.util.domain.DomainSuffix;

/**
 * Adds the Top level domain extensions to the index
 * 
 * @author Enis Soztutar &lt;enis.soz.nutch@gmail.com&gt;
 */
public class TLDIndexingFilter implements IndexingFilter {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Configuration conf;

  public NutchDocument filter(NutchDocument doc, Parse parse, Text urlText,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {

    try {
      URL url = new URL(urlText.toString());
      DomainSuffix d = URLUtil.getDomainSuffix(url);

      doc.add("tld", d.getDomain());

    } catch (Exception ex) {
      LOG.warn(ex.toString());
    }

    return doc;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }
}

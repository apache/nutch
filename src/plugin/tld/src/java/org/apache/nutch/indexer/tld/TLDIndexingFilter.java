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

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.URLUtil;
import org.apache.nutch.util.domain.DomainSuffix;

/**
 * Adds the Top level domain extensions to the index
 * 
 * @author Enis Soztutar &lt;enis.soz.nutch@gmail.com&gt;
 */
public class TLDIndexingFilter implements IndexingFilter {
  public static final Logger LOG = LoggerFactory.getLogger(TLDIndexingFilter.class);

  private Configuration conf;

  private static final Collection<Field> fields = new ArrayList<Field>();
  
  @Override
  public NutchDocument filter(NutchDocument doc, String url, WebPage page)
      throws IndexingException {
    try {
      URL _url = new URL(url);
      DomainSuffix d = URLUtil.getDomainSuffix(_url);
      doc.add("tld", d.getDomain());
    } catch (Exception ex) {
      LOG.warn("Exception in TLDIndexingFilter",ex);
    }

    return doc;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public Collection<Field> getFields() {
    return fields;
  }
}

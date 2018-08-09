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
package org.apache.nutch.indexer.subcollection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;

import org.apache.nutch.parse.Parse;
import org.apache.nutch.util.NutchConfiguration;

import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;

import org.apache.nutch.collection.CollectionManager;
import org.apache.nutch.collection.Subcollection;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;

public class SubcollectionIndexingFilter extends Configured implements
    IndexingFilter {

  private Configuration conf;

  public SubcollectionIndexingFilter() {
    super(NutchConfiguration.create());
  }

  public SubcollectionIndexingFilter(Configuration conf) {
    super(conf);
  }

  /**
   * @param conf
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    fieldName = conf.get("subcollection.default.fieldname", "subcollection");
    metadataSource = conf.get("subcollection.metadata.source", "subcollection");
  }

  /**
   * @return Configuration
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Doc field name
   */
  public static String fieldName = "subcollection";
  
  /**
   * Metadata source field name
   */
  public static String metadataSource = "subcollection";

  /**
   * "Mark" document to be a part of subcollection
   * 
   * @param doc
   * @param url
   */
  private void addSubCollectionField(NutchDocument doc, String url) {
    for (Subcollection coll : CollectionManager.getCollectionManager(getConf())
        .getSubCollections(url)) {
      if (coll.getKey() == null) {
        doc.add(fieldName, coll.getName());
      } else {
        doc.add(coll.getKey(), coll.getName());
      }
    }
  }

  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {
    // Check for subcollection overrride in HTML metadata
    String subcollection = parse.getData().getMeta(metadataSource);
    if (subcollection != null) {
      subcollection = subcollection.trim();
      
      if (subcollection.length() > 0) {
        doc.add(fieldName, subcollection);
        return doc;
      }
    }
    
    String sUrl = url.toString();
    addSubCollectionField(doc, sUrl);
    return doc;
  }
}

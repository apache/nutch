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

package org.apache.nutch.anthelion.indexing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.anthelion.parsing.WdcParser;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds boolean field containsSem based on the parsed contetnt of the web page
 * (true if the web page contains semantic data, otherwise false)
 * 
 * @author Petar Ristoski (petar@dwslab.de)
 *
 */
public class TripleExtractor implements IndexingFilter {

  private static final Logger LOG = LoggerFactory.getLogger(TripleExtractor.class);
  private Configuration conf;

  // implements the filter-method which gives you access to important Objects
  // like NutchDocument
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks) {
    LOG.info("-------->>>>> WE ARE IN THE INDExer-------------------");

    String containsSem = "false";

    containsSem = parse.getData().getMeta(WdcParser.META_CONTAINS_SEM);

    // we don't have to add the triples in a separate field as they are
    // already in the content field
    // String triples = "";
    // triples = parse.getText();
    // doc.add("triples", triples);

    // // check if the father contains sem data
    // boolean semFather = false;
    // try {
    // semFather =
    // Boolean.parseBoolean(datum.getMetaData().get(WdcParser.META_CONTAINS_SEM_FATHER).toString());
    //
    // } catch (Exception e) {
    // LOG.error("CANNOT PROCESS THE FATHER SEM FIELD" + e.getMessage());
    // }

    // adds the new field to the document
    doc.add("containsSem", containsSem);
    return doc;
  }

  // Boilerplate
  public Configuration getConf() {
    return conf;
  }

  // Boilerplate
  public void setConf(Configuration conf) {

    this.conf = conf;
    // this.conf.addResource("nutch-anth.xml");
  }
}
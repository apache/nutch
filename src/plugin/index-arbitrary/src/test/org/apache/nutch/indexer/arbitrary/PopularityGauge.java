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

package org.apache.nutch.indexer.arbitrary;

import org.apache.nutch.parse.Parse;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.hadoop.io.Text;

import java.io.PrintStream;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

public class PopularityGauge {

  private static PrintStream out = System.out;
  private String words;
  private NutchDocument doc;
  private Parse parse;
  private CrawlDatum datum;
  private Inlinks inlinks;
  private double popularityBoost;

  public PopularityGauge(String args[],
	      NutchDocument docIn,
              Parse parseIn,
              Text urlIn,
              CrawlDatum datumIn,
              Inlinks inlinksIn){
    super();
    doc = docIn;
    inlinks = inlinksIn;
    datum = datumIn;
  }

  public double getPopularityBoost() {
    popularityBoost = (Double) doc.getFieldValue("popularityBoost");
    if (datum.getStatus() == CrawlDatum.STATUS_FETCH_SUCCESS) {
      Set anchorSet = new HashSet<>(Arrays.asList(inlinks.getAnchors()));
      if(anchorSet.contains("dinosaur")){
        popularityBoost = popularityBoost + 0.50;
      } else {
	popularityBoost = popularityBoost - 0.20;
      }
      if (anchorSet.contains("baseball")) {
        popularityBoost = popularityBoost + 0.25;
      } else {
	popularityBoost = popularityBoost - 0.15;
      }
      if (anchorSet.contains("source code")) {
        popularityBoost = popularityBoost + 0.25;
      } else {
	popularityBoost = popularityBoost - 0.15;
      }
    }
    return popularityBoost;
  }

  public static void main(String[] args,
			  NutchDocument doc,
                          Parse parse,
			  Text url,
                          CrawlDatum datum,
                          Inlinks inlinks) {
    
    PopularityGauge popGauge = new PopularityGauge(args,doc,parse,url,datum,inlinks);
    out.println(popGauge.getPopularityBoost());
  }
}

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

package org.apache.nutch.scoring.tld;

import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.lucene.document.Document;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.util.domain.DomainSuffix;
import org.apache.nutch.util.domain.DomainSuffixes;


/**
 * Scoring filter to boost tlds.
 * @author Enis Soztutar &lt;enis.soz.nutch@gmail.com&gt;
 */
public class TLDScoringFilter implements ScoringFilter {

  private Configuration conf;
  private DomainSuffixes tldEntries;

  public TLDScoringFilter() {
    tldEntries = DomainSuffixes.getInstance();
  }

  public float indexerScore(Text url, Document doc, CrawlDatum dbDatum,
      CrawlDatum fetchDatum, Parse parse, Inlinks inlinks, float initScore)
      throws ScoringFilterException {

    String[] tlds = doc.getValues("tld");
    float boost = 1.0f;

    if(tlds != null) {
      for(String tld : tlds) {
        DomainSuffix entry = tldEntries.get(tld);
        if(entry != null)
          boost *= entry.getBoost();
      }
    }
    return initScore * boost;
  }

  public CrawlDatum distributeScoreToOutlink(Text fromUrl, Text toUrl,
      ParseData parseData, CrawlDatum target, CrawlDatum adjust, int allCount,
      int validCount) throws ScoringFilterException {
    return adjust;
  }

  public float generatorSortValue(Text url, CrawlDatum datum, float initSort)
      throws ScoringFilterException {
    return initSort;
  }

  public void initialScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
  }

  public void injectedScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
  }

  public void passScoreAfterParsing(Text url, Content content, Parse parse)
      throws ScoringFilterException {
  }

  public void passScoreBeforeParsing(Text url, CrawlDatum datum, Content content)
      throws ScoringFilterException {
  }

  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum,
      List inlinked) throws ScoringFilterException {
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl, ParseData parseData, 
          Collection<Entry<Text, CrawlDatum>> targets, CrawlDatum adjust,
          int allCount) throws ScoringFilterException {
    return adjust;
  }

}

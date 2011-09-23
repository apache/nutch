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
package org.apache.nutch.scoring.link;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;

public class LinkAnalysisScoringFilter
  implements ScoringFilter {

  private Configuration conf;
  private float normalizedScore = 1.00f;

  public LinkAnalysisScoringFilter() {

  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    normalizedScore = conf.getFloat("link.analyze.normalize.score", 1.00f);
  }

  public CrawlDatum distributeScoreToOutlinks(Text fromUrl,
    ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
    CrawlDatum adjust, int allCount)
    throws ScoringFilterException {
    return adjust;
  }

  public float generatorSortValue(Text url, CrawlDatum datum, float initSort)
    throws ScoringFilterException {
    return datum.getScore() * initSort;
  }

  public float indexerScore(Text url, NutchDocument doc, CrawlDatum dbDatum,
    CrawlDatum fetchDatum, Parse parse, Inlinks inlinks, float initScore)
    throws ScoringFilterException {
    return (normalizedScore * dbDatum.getScore());
  }

  public void initialScore(Text url, CrawlDatum datum)
    throws ScoringFilterException {
    datum.setScore(0.0f);
  }

  public void injectedScore(Text url, CrawlDatum datum)
    throws ScoringFilterException {
  }

  public void passScoreAfterParsing(Text url, Content content, Parse parse)
    throws ScoringFilterException {
    parse.getData().getContentMeta().set(Nutch.SCORE_KEY,
      content.getMetadata().get(Nutch.SCORE_KEY));
  }

  public void passScoreBeforeParsing(Text url, CrawlDatum datum, Content content)
    throws ScoringFilterException {
    content.getMetadata().set(Nutch.SCORE_KEY, "" + datum.getScore());
  }

  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum,
    List<CrawlDatum> inlinked)
    throws ScoringFilterException {
    // nothing to do
  }

}

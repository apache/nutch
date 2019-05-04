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
package org.apache.nutch.scoring;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.protocol.Content;

/**
 * Creates and caches {@link ScoringFilter} implementing plugins.
 * 
 * @author Andrzej Bialecki
 */
public class ScoringFilters extends Configured implements ScoringFilter {

  private ScoringFilter[] filters;

  public ScoringFilters(Configuration conf) {
    super(conf);
    this.filters = (ScoringFilter[]) PluginRepository.get(conf)
        .getOrderedPlugins(ScoringFilter.class, ScoringFilter.X_POINT_ID,
            "scoring.filter.order");
  }

  /** Calculate a sort value for Generate. */
  public float generatorSortValue(Text url, CrawlDatum datum, float initSort)
      throws ScoringFilterException {
    for (int i = 0; i < this.filters.length; i++) {
      initSort = this.filters[i].generatorSortValue(url, datum, initSort);
    }
    return initSort;
  }

  /** Calculate a new initial score, used when adding newly discovered pages. */
  public void initialScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
    for (int i = 0; i < this.filters.length; i++) {
      this.filters[i].initialScore(url, datum);
    }
  }

  /** Calculate a new initial score, used when injecting new pages. */
  public void injectedScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
    for (int i = 0; i < this.filters.length; i++) {
      this.filters[i].injectedScore(url, datum);
    }
  }

  /** Calculate updated page score during CrawlDb.update(). */
  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum,
      List<CrawlDatum> inlinked) throws ScoringFilterException {
    for (int i = 0; i < this.filters.length; i++) {
      this.filters[i].updateDbScore(url, old, datum, inlinked);
    }
  }

  /** Calculate orphaned page score during CrawlDb.update(). */
  public void orphanedScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
    for (int i = 0; i < this.filters.length; i++) {
      this.filters[i].orphanedScore(url, datum);
    }
  }

  public void passScoreBeforeParsing(Text url, CrawlDatum datum, Content content)
      throws ScoringFilterException {
    for (int i = 0; i < this.filters.length; i++) {
      this.filters[i].passScoreBeforeParsing(url, datum, content);
    }
  }

  public void passScoreAfterParsing(Text url, Content content, Parse parse)
      throws ScoringFilterException {
    for (int i = 0; i < this.filters.length; i++) {
      this.filters[i].passScoreAfterParsing(url, content, parse);
    }
  }

  public CrawlDatum distributeScoreToOutlinks(Text fromUrl,
      ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
      CrawlDatum adjust, int allCount) throws ScoringFilterException {
    for (int i = 0; i < this.filters.length; i++) {
      adjust = this.filters[i].distributeScoreToOutlinks(fromUrl, parseData,
          targets, adjust, allCount);
    }
    return adjust;
  }

  public float indexerScore(Text url, NutchDocument doc, CrawlDatum dbDatum,
      CrawlDatum fetchDatum, Parse parse, Inlinks inlinks, float initScore)
      throws ScoringFilterException {
    for (int i = 0; i < this.filters.length; i++) {
      initScore = this.filters[i].indexerScore(url, doc, dbDatum, fetchDatum,
          parse, inlinks, initScore);
    }
    return initScore;
  }

}

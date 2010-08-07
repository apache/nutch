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
package org.apache.nutch.scoring;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.plugin.FieldPluggable;
import org.apache.nutch.storage.WebPage;

/**
 * A contract defining behavior of scoring plugins.
 *
 * A scoring filter will manipulate scoring variables in CrawlDatum and
 * in resulting search indexes. Filters can be chained in a specific order,
 * to provide multi-stage scoring adjustments.
 *
 * @author Andrzej Bialecki
 */
public interface ScoringFilter extends Configurable, FieldPluggable {
  /** The name of the extension point. */
  public final static String X_POINT_ID = ScoringFilter.class.getName();

  /**
   * Set an initial score for newly injected pages. Note: newly injected pages
   * may have no inlinks, so filter implementations may wish to set this
   * score to a non-zero value, to give newly injected pages some initial
   * credit.
   * @param url url of the page
   * @param page new page. Filters will modify it in-place.
   * @throws ScoringFilterException
   */
  public void injectedScore(String url, WebPage page) throws ScoringFilterException;

  /**
   * Set an initial score for newly discovered pages. Note: newly discovered pages
   * have at least one inlink with its score contribution, so filter implementations
   * may choose to set initial score to zero (unknown value), and then the inlink
   * score contribution will set the "real" value of the new page.
   * @param url url of the page
   * @param page
   * @throws ScoringFilterException
   */
  public void initialScore(String url, WebPage page) throws ScoringFilterException;

  /**
   * This method prepares a sort value for the purpose of sorting and
   * selecting top N scoring pages during fetchlist generation.
   * @param url url of the page
   * @param datum page row. Modifications will be persisted.
   * @param initSort initial sort value, or a value from previous filters in chain
   */
  public float generatorSortValue(String url, WebPage page, float initSort) throws ScoringFilterException;

  /**
   * Distribute score value from the current page to all its outlinked pages.
   * @param fromUrl url of the source page
   * @param row page row
   * @param scoreData A list of {@link OutlinkedScoreDatum}s for every outlink.
   * These {@link OutlinkedScoreDatum}s will be passed to
   * {@link #updateScore(String, OldWebTableRow, List)}
   * for every outlinked URL.
   * @param allCount number of all collected outlinks from the source page
   * @throws ScoringFilterException
   */
  public void distributeScoreToOutlinks(String fromUrl,
      WebPage page, Collection<ScoreDatum> scoreData,
      int allCount) throws ScoringFilterException;

  /**
   * This method calculates a new score during table update, based on the values contributed
   * by inlinked pages.
   * @param url url of the page
   * @param page
   * @param inlinked list of {@link OutlinkedScoreDatum}s for all inlinks pointing to this URL.
   * @throws ScoringFilterException
   */
  public void updateScore(String url, WebPage page, List<ScoreDatum> inlinkedScoreData)
  throws ScoringFilterException;

  /**
   * This method calculates a Lucene document boost.
   * @param url url of the page
   * @param doc document. NOTE: this already contains all information collected
   * by indexing filters. Implementations may modify this instance, in order to store/remove
   * some information.
   * @param row page row
   * @param initScore initial boost value for the Lucene document.
   * @return boost value for the Lucene document. This value is passed as an argument
   * to the next scoring filter in chain. NOTE: implementations may also express
   * other scoring strategies by modifying Lucene document directly.
   * @throws ScoringFilterException
   */
  public float indexerScore(String url, NutchDocument doc, WebPage page, float initScore)
  throws ScoringFilterException;
}

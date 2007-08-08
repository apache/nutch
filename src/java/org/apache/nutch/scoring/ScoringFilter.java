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
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.document.Document;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.plugin.Pluggable;
import org.apache.nutch.protocol.Content;

/**
 * A contract defining behavior of scoring plugins.
 * 
 * A scoring filter will manipulate scoring variables in CrawlDatum and
 * in resulting search indexes. Filters can be chained in a specific order,
 * to provide multi-stage scoring adjustments.
 * 
 * @author Andrzej Bialecki
 */
public interface ScoringFilter extends Configurable, Pluggable {
  /** The name of the extension point. */
  public final static String X_POINT_ID = ScoringFilter.class.getName();
  
  /**
   * Set an initial score for newly injected pages. Note: newly injected pages
   * may have no inlinks, so filter implementations may wish to set this 
   * score to a non-zero value, to give newly injected pages some initial
   * credit.
   * @param url url of the page
   * @param datum new datum. Filters will modify it in-place.
   * @throws ScoringFilterException
   */
  public void injectedScore(Text url, CrawlDatum datum) throws ScoringFilterException;
  
  /**
   * Set an initial score for newly discovered pages. Note: newly discovered pages
   * have at least one inlink with its score contribution, so filter implementations
   * may choose to set initial score to zero (unknown value), and then the inlink
   * score contribution will set the "real" value of the new page.
   * @param url url of the page
   * @param datum new datum. Filters will modify it in-place.
   * @throws ScoringFilterException
   */
  public void initialScore(Text url, CrawlDatum datum) throws ScoringFilterException;
  
  /**
   * This method prepares a sort value for the purpose of sorting and
   * selecting top N scoring pages during fetchlist generation.
   * @param url url of the page
   * @param datum page's datum, should not be modified
   * @param initSort initial sort value, or a value from previous filters in chain
   */
  public float generatorSortValue(Text url, CrawlDatum datum, float initSort) throws ScoringFilterException;
  
  /**
   * This method takes all relevant score information from the current datum
   * (coming from a generated fetchlist) and stores it into
   * {@link org.apache.nutch.protocol.Content} metadata.
   * This is needed in order to pass this value(s) to the mechanism that distributes it
   * to outlinked pages.
   * @param url url of the page
   * @param datum source datum. NOTE: modifications to this value are not persisted.
   * @param content instance of content. Implementations may modify this
   * in-place, primarily by setting some metadata properties.
   */
  public void passScoreBeforeParsing(Text url, CrawlDatum datum, Content content) throws ScoringFilterException;
  
  /**
   * Currently a part of score distribution is performed using only data coming
   * from the parsing process. We need this method in order to ensure the
   * presence of score data in these steps.
   * @param url page url
   * @param content original content. NOTE: modifications to this value are not persisted.
   * @param parse target instance to copy the score information to. Implementations
   * may modify this in-place, primarily by setting some metadata properties.
   */
  public void passScoreAfterParsing(Text url, Content content, Parse parse) throws ScoringFilterException;
  
  /**
   * Distribute score value from the current page to all its outlinked pages.
   * @param fromUrl url of the source page
   * @param parseData ParseData instance, which stores relevant score value(s)
   * in its metadata. NOTE: filters may modify this in-place, all changes will
   * be persisted.
   * @param targets &lt;url, CrawlDatum&gt; pairs. NOTE: filters can modify this in-place,
   * all changes will be persisted.
   * @param adjust a CrawlDatum instance, initially null, which implementations
   * may use to pass adjustment values to the original CrawlDatum. When creating
   * this instance, set its status to {@link CrawlDatum#STATUS_LINKED}.
   * @param allCount number of all collected outlinks from the source page
   * @return if needed, implementations may return an instance of CrawlDatum,
   * with status {@link CrawlDatum#STATUS_LINKED}, which contains adjustments
   * to be applied to the original CrawlDatum score(s) and metadata. This can
   * be null if not needed.
   * @throws ScoringFilterException
   */
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl, ParseData parseData, 
          Collection<Entry<Text, CrawlDatum>> targets, CrawlDatum adjust,
          int allCount) throws ScoringFilterException;

  /**
   * This method calculates a new score of CrawlDatum during CrawlDb update, based on the
   * initial value of the original CrawlDatum, and also score values contributed by
   * inlinked pages.
   * @param url url of the page
   * @param old original datum, with original score. May be null if this is a newly
   * discovered page. If not null, filters should use score values from this parameter
   * as the starting values - the <code>datum</code> parameter may contain values that are
   * no longer valid, if other updates occured between generation and this update.
   * @param datum the new datum, with the original score saved at the time when
   * fetchlist was generated. Filters should update this in-place, and it will be saved in
   * the crawldb.
   * @param inlinked (partial) list of CrawlDatum-s (with their scores) from
   * links pointing to this page, found in the current update batch.
   * @throws ScoringFilterException
   */
  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum, List<CrawlDatum> inlinked) throws ScoringFilterException;
  
  /**
   * This method calculates a Lucene document boost.
   * @param url url of the page
   * @param doc Lucene document. NOTE: this already contains all information collected
   * by indexing filters. Implementations may modify this instance, in order to store/remove
   * some information.
   * @param dbDatum current page from CrawlDb. NOTE: changes made to this instance
   * are not persisted.
   * @param fetchDatum datum from FetcherOutput (containing among others the fetching status)
   * @param parse parsing result. NOTE: changes made to this instance are not persisted.
   * @param inlinks current inlinks from LinkDb. NOTE: changes made to this instance are
   * not persisted.
   * @param initScore initial boost value for the Lucene document.
   * @return boost value for the Lucene document. This value is passed as an argument
   * to the next scoring filter in chain. NOTE: implementations may also express
   * other scoring strategies by modifying Lucene document directly.
   * @throws ScoringFilterException
   */
  public float indexerScore(Text url, Document doc, CrawlDatum dbDatum,
          CrawlDatum fetchDatum, Parse parse, Inlinks inlinks, float initScore) throws ScoringFilterException;
}

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

package org.apache.nutch.scoring.opic;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.hbase.HbaseColumn;
import org.apache.nutch.util.hbase.WebTableRow;
import org.apache.nutch.util.hbase.WebTableColumns;

/**
 * This plugin implements a variant of an Online Page Importance Computation
 * (OPIC) score, described in this paper:
 * <a href="http://www2003.org/cdrom/papers/refereed/p007/p7-abiteboul.html"/>
 * Abiteboul, Serge and Preda, Mihai and Cobena, Gregory (2003),
 * Adaptive On-Line Page Importance Computation
 * </a>.
 * 
 * @author Andrzej Bialecki
 */
public class OPICScoringFilter implements ScoringFilter {

  private final static Log LOG = LogFactory.getLog(OPICScoringFilter.class);
  
  private final static byte[] CASH_KEY = Bytes.toBytes("_csh_");
  
  private final static Set<HbaseColumn> COLUMNS = new HashSet<HbaseColumn>();
  
  static {
    COLUMNS.add(new HbaseColumn(WebTableColumns.METADATA, CASH_KEY));
    COLUMNS.add(new HbaseColumn(WebTableColumns.SCORE, CASH_KEY));
  }

  private Configuration conf;
  private float scoreInjected;
  private float scorePower;
  private float internalScoreFactor;
  private float externalScoreFactor;
  @SuppressWarnings("unused")
  private boolean countFiltered;

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    scoreInjected = conf.getFloat("db.score.injected", 1.0f);
    scorePower = conf.getFloat("indexer.score.power", 0.5f);
    internalScoreFactor = conf.getFloat("db.score.link.internal", 1.0f);
    externalScoreFactor = conf.getFloat("db.score.link.external", 1.0f);
    countFiltered = conf.getBoolean("db.score.count.filtered", false);
  }

  /** Set to the value defined in config, 1.0f by default. */
  @Override
  public void injectedScore(String url, WebTableRow row)
  throws ScoringFilterException {
    row.setScore(scoreInjected);
    row.putMeta(CASH_KEY, Bytes.toBytes(scoreInjected));
  }

  /** Set to 0.0f (unknown value) - inlink contributions will bring it to
   * a correct level. Newly discovered pages have at least one inlink. */
  @Override
  public void initialScore(String url, WebTableRow row) throws ScoringFilterException {
    row.setScore(0.0f);
    row.putMeta(CASH_KEY, Bytes.toBytes(0.0f));
  }

  /** Use {@link CrawlDatum#getScore()}. */
  @Override
  public float generatorSortValue(String url, WebTableRow row, float initSort) throws ScoringFilterException {
    return row.getScore() * initSort;
  }

  /** Increase the score by a sum of inlinked scores. */
  @Override
  public void updateScore(String url, WebTableRow row, List<ScoreDatum> inlinkedScoreData) {
    float adjust = 0.0f;
    for (ScoreDatum scoreDatum : inlinkedScoreData) {
      adjust += scoreDatum.getScore();
    }
    float oldScore = row.getScore();
    row.setScore(oldScore + adjust);
    float cash = Bytes.toFloat(row.getMeta(CASH_KEY));
    row.putMeta(CASH_KEY, Bytes.toBytes(cash + adjust));
  }

  /** Get cash on hand, divide it by the number of outlinks and apply. */
  @Override
  public void distributeScoreToOutlinks(String fromUrl,
      WebTableRow row, Collection<ScoreDatum> scoreData,
      int allCount) {
    float cash = Bytes.toFloat(row.getMeta(CASH_KEY));
    if (cash == 0) {
      return;
    }
    // TODO: count filtered vs. all count for outlinks
    float scoreUnit = cash / allCount;
    // internal and external score factor
    float internalScore = scoreUnit * internalScoreFactor;
    float externalScore = scoreUnit * externalScoreFactor;
    for (ScoreDatum scoreDatum : scoreData) {
      try {
        String toHost = new URL(scoreDatum.getUrl()).getHost();
        String fromHost = new URL(fromUrl.toString()).getHost();
        if(toHost.equalsIgnoreCase(fromHost)){
          scoreDatum.setScore(internalScore);
        } else {
          scoreDatum.setScore(externalScore);
        }
      } catch (MalformedURLException e) {
        e.printStackTrace(LogUtil.getWarnStream(LOG));
        scoreDatum.setScore(externalScore);
      }
    }
    // reset cash to zero
    row.putMeta(CASH_KEY, Bytes.toBytes(0.0f));
  }

  /** Dampen the boost value by scorePower.*/
  public float indexerScore(String url, NutchDocument doc, WebTableRow row, float initScore) {
    return (float)Math.pow(row.getScore(), scorePower) * initScore;
  }

  @Override
  public Collection<HbaseColumn> getColumns() {
    return COLUMNS;
  }
}

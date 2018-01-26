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
package org.apache.nutch.scoring.orphan;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.scoring.AbstractScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orphan scoring filter that determines whether a page has become orphaned,
 * e.g. it has no more other pages linking to it. If a page hasn't been linked
 * to after markGoneAfter seconds, the page is marked as gone and is then
 * removed by an indexer. If a page hasn't been linked to after markOrphanAfter
 * seconds, the page is removed from the CrawlDB.
 */
public class OrphanScoringFilter extends AbstractScoringFilter {
  private static final Logger LOG = LoggerFactory
      .getLogger(OrphanScoringFilter.class);

  public static Text ORPHAN_KEY_WRITABLE = new Text("_orphan_");

  private Configuration conf;
  private static int DEFAULT_GONE_TIME = 30 * 24 * 60 * 60;
  private static int DEFAULT_ORPHAN_TIME = 40 * 24 * 60 * 60;

  private long markGoneAfter = DEFAULT_GONE_TIME;
  private long markOrphanAfter = DEFAULT_ORPHAN_TIME;

  public void setConf(Configuration conf) {
    markGoneAfter = conf.getInt("scoring.orphan.mark.gone.after",
        DEFAULT_GONE_TIME);
    markOrphanAfter = conf.getInt("scoring.orphan.mark.orphan.after",
        DEFAULT_ORPHAN_TIME);
    if (markGoneAfter > markOrphanAfter) {
      LOG.warn("OrphanScoringFilter: the time span after which pages are marked"
          + " as gone is larger than that to mark pages as orphaned"
          + " (scoring.orphan.mark.gone.after > scoring.orphan.mark.orphan.after):"
          + " This disables marking pages as gone.");
    }
  }

  /**
   * Used for orphan control.
   *
   * @param Text url of the record
   * @param CrawlDatum old CrawlDatum
   * @param CrawlDatum new CrawlDatum
   * @param List<CrawlDatum> list of inlinked CrawlDatums
   * @return void
   */
  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum,
      List<CrawlDatum> inlinks) throws ScoringFilterException {

    int now = (int)(System.currentTimeMillis() / 1000);

    // Are there inlinks for this record?
    if (inlinks.size() > 0) {
      // Set the last time we have seen this link to NOW
      datum.getMetaData().put(ORPHAN_KEY_WRITABLE,
          new IntWritable(now));
    } else {
      orphanedScore(url, datum);
    }
  }

  public void orphanedScore(Text url, CrawlDatum datum) {
    // Already has an orphaned time?
    if (datum.getMetaData().containsKey(ORPHAN_KEY_WRITABLE)) {
      // Get the last time this hyperlink was inlinked
      IntWritable writable = (IntWritable)datum.getMetaData()
          .get(ORPHAN_KEY_WRITABLE);
      int lastInlinkTime = writable.get();
      int now = (int) (System.currentTimeMillis() / 1000);
      int elapsedSinceLastInLinkTime = now - lastInlinkTime;

      if (elapsedSinceLastInLinkTime > markOrphanAfter) {
        // Mark as orphan so we can permanently delete it
        datum.setStatus(CrawlDatum.STATUS_DB_ORPHAN);
      } else if (elapsedSinceLastInLinkTime > markGoneAfter) {
        // Mark as gone so the indexer can remove it
        datum.setStatus(CrawlDatum.STATUS_DB_GONE);
      }
    }
  }

}

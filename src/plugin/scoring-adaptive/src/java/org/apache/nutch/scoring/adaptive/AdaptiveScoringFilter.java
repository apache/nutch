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

package org.apache.nutch.scoring.adaptive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.scoring.AbstractScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;

/**
 * Scoring filter adaptive to page score, fetch status and time.
 * 
 * <p>
 * The generator score of a page depends in a configurable way on
 * <ul>
 * <li>the page score</li>
 * <li>the crawl status (fetched, not modified, redirect, gone)</li>
 * <li>the time elapsed since the scheduled fetch time</li>
 * </ul>
 * </p>
 * 
 * <p>
 * While {@link org.apache.nutch.crawl.FetchSchedule}s set a fix (re)retch time
 * immediately after a page has been fetched, this scoring plugin allows a more
 * dynamic selection how many and which pages should be generated based on the
 * current configuration, independent from previous settings, by adjusting
 * <ul>
 * <li>the plugin parameters in accordance with</li>
 * <li><code>-topN</code> and <code>generate.min.score</code></li>
 * <li><code>generate.max.count</code></li>
 * </ul>
 * 
 * <p>
 * The plugin is thought for large crawls where there are far more URLs than can
 * be fetched and taking a good sample is mandatory. Sampling is, of course,
 * usually based on the page score - relevant pages with a high score are
 * fetched with higher probability. However, a dynamic rotation of generated
 * items helps to avoid that the same page with a slightly higher score is
 * fetched again while others are still waiting to be queued. It also allows to
 * adjust the probabilities that gone or not modified pages are refetched.
 * </p>
 * 
 * [TODO:experimental]
 * 
 * The plugin also includes heuristics to &quot;retire&quot; pages to status
 * db_orphan if they fail to fetch or are duplicates and are not seen in seeds
 * or via inlinks (cf. the plugin scoring-orphan).
 * 
 */
public class AdaptiveScoringFilter extends AbstractScoringFilter {

  private final static Logger LOG = LoggerFactory
      .getLogger(AdaptiveScoringFilter.class);

  /**
   * Generator sort value factor for pages to be (re)fetched based on the time
   * (in days) elapsed since the scheduled fetch time:
   * 
   * <pre>
   * generator_sort_value += (factor * days_elapsed)
   * </pre>
   */
  public static final String ADAPTIVE_FETCH_TIME_SORT_FACTOR = "scoring.adaptive.factor.fetchtime";

  public static final String ADAPTIVE_STATUS_SORT_FACTOR_FILE = "scoring.adaptive.sort.by_status.file";

  /**
   * Factor penalizing pages not successfully fetched for each failed fetch
   * trial:
   * 
   * <pre>
   * generator_sort_value -= (penalty * retries_since_fetch)
   * </pre>
   */
  public static final String ADAPTIVE_FETCH_RETRY_PENALTY = "scoring.adaptive.penalty.fetch_retry";

  /**
   * Boost recently injected URLs (injected within the last 7 days):
   * 
   * <pre>
   * generator_sort_value += injected_boost
   * </pre>
   */
  public static final String ADAPTIVE_INJECTED_BOOST = "scoring.adaptive.boost.injected";

  /*
   * Time span (in minutes) after which a page not seen anymore by inlink or
   * seed is marked as orpaned.
   */
  public static final String ADAPTIVE_ORPHAN_TIME = "scoring.adaptive.mark.orphan.after";

  /*
   * Time span (in minutes) after which a &quot;gone&quot; page not seen anymore
   * by inlink or seed is marked as orpaned. Also duplicates and unfetched pages
   * with a retry count >= 3 are considered as &quot;gone&quot;.
   */
  public static final String ADAPTIVE_ORPHAN_TIME_GONE = "scoring.adaptive.mark.gone.orphan.after";

  /**
   * Time stamp (in minutes) when a page has been &quot;seen&quot; the last
   * time, either as link or as seed URL.
   */
  public static final String LAST_SEEN_TIME = "_lst_";
  public static final Text WRITABLE_LAST_SEEN_TIME = new Text(LAST_SEEN_TIME);

  /**
   * Time stamp (in minutes) when a page has been successfully fetched.
   */
  public static final String SUCCESSFUL_FETCH_TIME = "_sft_";
  public static final Text WRITABLE_SUCCESSFUL_FETCH_TIME = new Text(SUCCESSFUL_FETCH_TIME);

  private Configuration conf;

  /**
   * Current time in milliseconds used to calculate time elapsed since a page
   * should have been (re)fetched while generating fetch lists. Can be
   * set/overwritten from {@link Generator} by option -adddays (internally set
   * via @{link Generator.GENERATOR_CUR_TIME}.
   */
  private long curTime;

  private float adaptiveFetchTimeSort;
  private float adaptiveFetchRetryPenalty;
  private float adaptiveBoostInjected;

  private Map<Byte, Float> statusSortMap = new TreeMap<Byte, Float>();

  private FetchSchedule schedule;
  int nowMinutes;
  int orphanTimeGone;
  int orphanTimeAny;

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    curTime = conf.getLong(Generator.GENERATOR_CUR_TIME,
        System.currentTimeMillis());
    adaptiveFetchTimeSort = conf.getFloat(ADAPTIVE_FETCH_TIME_SORT_FACTOR,
        .01f);
    adaptiveFetchRetryPenalty = conf.getFloat(ADAPTIVE_FETCH_RETRY_PENALTY,
        .1f);
    adaptiveBoostInjected = conf.getFloat(ADAPTIVE_INJECTED_BOOST, .2f);
    String adaptiveStatusSortFile = conf.get(ADAPTIVE_STATUS_SORT_FACTOR_FILE,
        "adaptive-scoring.txt");
    Reader adaptiveStatusSortReader = conf
        .getConfResourceAsReader(adaptiveStatusSortFile);
    try {
      readSortFile(adaptiveStatusSortReader);
    } catch (IOException e) {
      LOG.error("Failed to read adaptive scoring file {}: {}",
          adaptiveStatusSortFile, StringUtils.stringifyException(e));
    }

    // orphan detection
    schedule = FetchScheduleFactory.getFetchSchedule(conf);
    nowMinutes = (int) (System.currentTimeMillis() / (60000));
    int orphanTimeSpanAny = conf.getInt(ADAPTIVE_ORPHAN_TIME,
        60 * 24 * 30 * 12);
    orphanTimeAny = nowMinutes - orphanTimeSpanAny;
    int orphanTimeSpanGone = conf.getInt(ADAPTIVE_ORPHAN_TIME_GONE,
        60 * 24 * 30 * 4);
    orphanTimeGone = nowMinutes - orphanTimeSpanGone;
  }

  private void readSortFile(Reader sortFileReader) throws IOException {
    BufferedReader reader = new BufferedReader(sortFileReader);
    String line = null;
    String[] splits = null;
    while ((line = reader.readLine()) != null) {
      if (line.matches("^\\s*$") || line.startsWith("#"))
        continue; // skip empty lines and comments
      splits = line.split("\t");
      if (splits.length < 2) {
        LOG.warn("Invalid line (expected format <status> \t <sortval>): {}",
            line);
        continue;
      }
      float value;
      try {
        value = Float.parseFloat(splits[1]);
      } catch (NumberFormatException e) {
        LOG.warn("Invalid sort value `{}' in line: {}", splits[1], line);
        continue;
      }
      byte status = -1;
      for (Entry<Byte, String> entry : CrawlDatum.statNames.entrySet()) {
        if (entry.getValue().equals(splits[0])) {
          status = entry.getKey();
          statusSortMap.put(status, value);
          break;
        }
      }
      if (status == -1) {
        LOG.warn("Invalid status `{}' in line: {}", splits[0], line);
      }
    }
  }

  /** Add injected timestamp to metadata */
  public void injectedScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
    datum.getMetaData().put(WRITABLE_LAST_SEEN_TIME,
        new IntWritable(nowMinutes));
  }

  /**
   * Use {@link CrawlDatum#getScore()} but be adaptive to page status and
   * fetch time.
   */
  public float generatorSortValue(Text url, CrawlDatum datum, float initSort)
      throws ScoringFilterException {
    initSort *= datum.getScore();
    long fetchTime = datum.getFetchTime();
    byte status = datum.getStatus();
    long daysSinceScheduledFetch = (curTime - fetchTime) / 86400000;
    if (adaptiveFetchTimeSort > 0.0f) {
      // boost/penalize by time elapsed since the scheduled fetch time
      float fetchTimeSort = (float) (adaptiveFetchTimeSort
          * daysSinceScheduledFetch);
      initSort += fetchTimeSort;
    }
    if (statusSortMap.containsKey(status)) {
      // boost/penalize by fetch status
      initSort += statusSortMap.get(status);
    }
    if (status == CrawlDatum.STATUS_DB_UNFETCHED) {
      if (datum.getRetriesSinceFetch() > 0) {
        // penalize by fetch retry count
        initSort -= datum.getRetriesSinceFetch() * adaptiveFetchRetryPenalty;
      } else if (daysSinceScheduledFetch <= 7) {
        // boost recently injected URLs
        // - status unfetched
        // - retry count == 0
        // - scheduled fetch with the last 7 days
        initSort += adaptiveBoostInjected;
      }
    }
    return initSort;
  }

  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum,
      List<CrawlDatum> inlinks) throws ScoringFilterException {

    // Are there inlinks for this record?
    if (inlinks.size() > 0) {
      // Set the last time we have seen this link to the fetch time
      int fetchTimeMinutes = (int) (schedule.calculateLastFetchTime(datum)
          / 60000);
      datum.getMetaData().put(WRITABLE_LAST_SEEN_TIME,
          new IntWritable(fetchTimeMinutes));
    } else {
      orphanedScore(url, datum, old);
    }
  }

  public void orphanedScore(Text url, CrawlDatum datum) {
    orphanedScore(url, datum, null);
  }

  private void orphanedScore(Text url, CrawlDatum datum, CrawlDatum old) {
    if (datum.getMetaData().containsKey(WRITABLE_LAST_SEEN_TIME)) {
      IntWritable writable = (IntWritable) datum.getMetaData()
          .get(WRITABLE_LAST_SEEN_TIME);
      int lastSeenMinutes = writable.get();
      if (lastSeenMinutes > this.orphanTimeGone) {
        // keep in any case
        // (last seen time after mark-as-orphan-if-gone time)
      } else if (lastSeenMinutes < this.orphanTimeAny) {
        // (last seen time before mark-any-as-orphan time)
        datum.setStatus(CrawlDatum.STATUS_DB_ORPHAN);
      } else if (pageIsGone(datum)) {
        // (last seen time before mark-as-orphan-if-gone time
        //  but after mark-any-as-orphan time)
        datum.setStatus(CrawlDatum.STATUS_DB_ORPHAN);
      }
    } else {
      // (for a transition period) also mark pages as orphaned which failed to
      // fetch a second time but have no "last seen" marker yet
      if (old != null && pageIsGone(old) && pageIsGone(datum)) {
        datum.setStatus(CrawlDatum.STATUS_DB_ORPHAN);
      }
    }
  }

  private static boolean pageIsGone(CrawlDatum datum) {
    byte status = datum.getStatus();
    if (status == CrawlDatum.STATUS_DB_GONE
        || status == CrawlDatum.STATUS_DB_DUPLICATE
        || (status == CrawlDatum.STATUS_DB_UNFETCHED
            && datum.getRetriesSinceFetch() >= 3)) {
      return true;
    }
    return false;
  }

}

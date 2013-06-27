/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.crawl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.WebPageWritable;
import org.apache.gora.mapreduce.GoraReducer;

public class DbUpdateReducer
extends GoraReducer<UrlWithScore, NutchWritable, String, WebPage> {

  public static final String CRAWLDB_ADDITIONS_ALLOWED = "db.update.additions.allowed";	
	
  public static final Logger LOG = DbUpdaterJob.LOG;

  private int retryMax;
  private boolean additionsAllowed;
  private int maxInterval;
  private FetchSchedule schedule;
  private ScoringFilters scoringFilters;
  private List<ScoreDatum> inlinkedScoreData = new ArrayList<ScoreDatum>();
  private int maxLinks;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    retryMax = conf.getInt("db.fetch.retry.max", 3);
    additionsAllowed = conf.getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);
    maxInterval = conf.getInt("db.fetch.interval.max", 0 );
    schedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
    maxLinks = conf.getInt("db.update.max.inlinks", 10000);
  }

  @Override
  protected void reduce(UrlWithScore key, Iterable<NutchWritable> values,
      Context context) throws IOException, InterruptedException {
    String keyUrl = key.getUrl().toString();

    WebPage page = null;
    inlinkedScoreData.clear();
    
    for (NutchWritable nutchWritable : values) {
      Writable val = nutchWritable.get();
      if (val instanceof WebPageWritable) {
        page = ((WebPageWritable) val).getWebPage();
      } else {
        inlinkedScoreData.add((ScoreDatum) val);
        if (inlinkedScoreData.size() >= maxLinks) {
          LOG.info("Limit reached, skipping further inlinks for " + keyUrl);
          break;
        }
      }
    }
    String url;
    try {
      url = TableUtil.unreverseUrl(keyUrl);
    } catch (Exception e) {
      // this can happen because a newly discovered malformed link
      // may slip by url filters
      // TODO: Find a better solution
      return;
    }

    if (page == null) { // new row
      if (!additionsAllowed) {
        return;
      }
      page = new WebPage();
      schedule.initializeSchedule(url, page);
      page.setStatus(CrawlStatus.STATUS_UNFETCHED);
      try {
        scoringFilters.initialScore(url, page);
      } catch (ScoringFilterException e) {
        page.setScore(0.0f);
      }
    } else {
      byte status = (byte)page.getStatus();
      switch (status) {
      case CrawlStatus.STATUS_FETCHED:         // succesful fetch
      case CrawlStatus.STATUS_REDIR_TEMP:      // successful fetch, redirected
      case CrawlStatus.STATUS_REDIR_PERM:
      case CrawlStatus.STATUS_NOTMODIFIED:     // successful fetch, notmodified
        int modified = FetchSchedule.STATUS_UNKNOWN;
        if (status == CrawlStatus.STATUS_NOTMODIFIED) {
          modified = FetchSchedule.STATUS_NOTMODIFIED;
        }
        ByteBuffer prevSig = page.getPrevSignature();
        ByteBuffer signature = page.getSignature();
        if (prevSig != null && signature != null) {
          if (SignatureComparator.compare(prevSig, signature) != 0) {
            modified = FetchSchedule.STATUS_MODIFIED;
          } else {
            modified = FetchSchedule.STATUS_NOTMODIFIED;
          }
        }
        long fetchTime = page.getFetchTime();
        long prevFetchTime = page.getPrevFetchTime();
        long modifiedTime = page.getModifiedTime();
        long prevModifiedTime = page.getPrevModifiedTime();

        schedule.setFetchSchedule(url, page, prevFetchTime, prevModifiedTime,
            fetchTime, modifiedTime, modified);
        if (maxInterval < page.getFetchInterval())
          schedule.forceRefetch(url, page, false);
        break;
      case CrawlStatus.STATUS_RETRY:
        schedule.setPageRetrySchedule(url, page, 0L, page.getPrevModifiedTime(), page.getFetchTime());
        if (page.getRetriesSinceFetch() < retryMax) {
          page.setStatus(CrawlStatus.STATUS_UNFETCHED);
        } else {
          page.setStatus(CrawlStatus.STATUS_GONE);
        }
        break;
      case CrawlStatus.STATUS_GONE:
        schedule.setPageGoneSchedule(url, page, 0L, page.getPrevModifiedTime(), page.getFetchTime());
        break;
      }
    }

    if (page.getInlinks() != null) {
      page.getInlinks().clear();
    }
    for (ScoreDatum inlink : inlinkedScoreData) {
      page.putToInlinks(new Utf8(inlink.getUrl()), new Utf8(inlink.getAnchor()));
    }

    // Distance calculation.
    // Retrieve smallest distance from all inlinks distances
    // Calculate new distance for current page: smallest inlink distance plus 1.
    // If the new distance is smaller than old one (or if old did not exist yet),
    // write it to the page.
    int smallestDist=Integer.MAX_VALUE;
    for (ScoreDatum inlink : inlinkedScoreData) {
      int inlinkDist = inlink.getDistance();
      if (inlinkDist < smallestDist) {
        smallestDist=inlinkDist;
      }
      page.putToInlinks(new Utf8(inlink.getUrl()), new Utf8(inlink.getAnchor()));
    }
    if (smallestDist != Integer.MAX_VALUE) {
      int oldDistance=Integer.MAX_VALUE;
      Utf8 oldDistUtf8 = page.getFromMarkers(DbUpdaterJob.DISTANCE);
      if (oldDistUtf8 != null)oldDistance=Integer.parseInt(oldDistUtf8.toString());
      int newDistance = smallestDist+1;
      if (newDistance < oldDistance) {
        page.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8(Integer.toString(newDistance)));
      }
    }

    try {
      scoringFilters.updateScore(url, page, inlinkedScoreData);
    } catch (ScoringFilterException e) {
      LOG.warn("Scoring filters failed with exception " +
                StringUtils.stringifyException(e));
    }

    // clear markers
    // But only delete when they exist. This is much faster for the underlying
    // store. The markers are on the input anyway.
    if (page.getFromMetadata(FetcherJob.REDIRECT_DISCOVERED) != null) {
      page.removeFromMetadata(FetcherJob.REDIRECT_DISCOVERED);
    }
    Mark.GENERATE_MARK.removeMarkIfExist(page);
    Mark.FETCH_MARK.removeMarkIfExist(page);
    Utf8 parse_mark = Mark.PARSE_MARK.checkMark(page);
    if (parse_mark != null) {
      Mark.UPDATEDB_MARK.putMark(page, parse_mark);
      Mark.PARSE_MARK.removeMark(page);
    }

    context.write(keyUrl, page);
  }

}

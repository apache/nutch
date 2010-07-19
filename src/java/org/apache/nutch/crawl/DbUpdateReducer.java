package org.apache.nutch.crawl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
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
import org.gora.mapreduce.GoraReducer;

public class DbUpdateReducer
extends GoraReducer<String, NutchWritable, String, WebPage> {

  public static final String CRAWLDB_ADDITIONS_ALLOWED = "db.update.additions.allowed";	
	
  public static final Log LOG = DbUpdaterJob.LOG;

  private int retryMax;
  private boolean additionsAllowed;
  private int maxInterval;
  private FetchSchedule schedule;
  private ScoringFilters scoringFilters;
  private List<ScoreDatum> inlinkedScoreData = new ArrayList<ScoreDatum>();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    retryMax = conf.getInt("db.fetch.retry.max", 3);
    additionsAllowed = conf.getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);
    maxInterval = conf.getInt("db.fetch.interval.max", 0 );
    schedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
  }

  @Override
  protected void reduce(String key, Iterable<NutchWritable> values,
      Context context) throws IOException, InterruptedException {

    WebPage page = null;
    inlinkedScoreData.clear();

    for (NutchWritable nutchWritable : values) {
      Writable val = nutchWritable.get();
      if (val instanceof WebPageWritable) {
        page = ((WebPageWritable) val).getWebPage();
      } else {
        inlinkedScoreData.add((ScoreDatum) val);
      }
    }
    String url;
    try {
      url = TableUtil.unreverseUrl(key);
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
      if (page.getMetadata().containsKey(FetcherJob.REDIRECT_DISCOVERED)
            && !page.isReadable(WebPage.Field.STATUS.getIndex())) {
        // this row is marked during fetch as the destination of a redirect
        // but does not contain anything else, so we initialize it.
        page.setStatus(CrawlStatus.STATUS_UNFETCHED);
        schedule.initializeSchedule(url, page);
        try {
          scoringFilters.initialScore(url, page);
        } catch (ScoringFilterException e) {
          page.setScore(0.0f);
        }
      } else { // update row
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
            if (SignatureComparator.compare(prevSig.array(), signature.array()) != 0) {
              modified = FetchSchedule.STATUS_MODIFIED;
            } else {
              modified = FetchSchedule.STATUS_NOTMODIFIED;
            }
          }
          long fetchTime = page.getFetchTime();
          long prevFetchTime = page.getPrevFetchTime();
          long modifiedTime = page.getModifiedTime();

          schedule.setFetchSchedule(url, page, prevFetchTime, 0L,
              fetchTime, modifiedTime, modified);
          if (maxInterval < page.getFetchInterval())
            schedule.forceRefetch(url, page, false);
          break;
        case CrawlStatus.STATUS_RETRY:
          schedule.setPageRetrySchedule(url, page, 0L, 0L, page.getFetchTime());
          if (page.getRetriesSinceFetch() < retryMax) {
            page.setStatus(CrawlStatus.STATUS_UNFETCHED);
          } else {
            page.setStatus(CrawlStatus.STATUS_GONE);
          }
          break;
        case CrawlStatus.STATUS_GONE:
          schedule.setPageGoneSchedule(url, page, 0L, 0L, page.getFetchTime());
          break;
        }
      }
    }

    if (page.getInlinks() != null) {
      page.getInlinks().clear();
    }
    for (ScoreDatum inlink : inlinkedScoreData) {
      page.putToInlinks(new Utf8(inlink.getUrl()), new Utf8(inlink.getAnchor()));
    }

    try {
      scoringFilters.updateScore(url, page, inlinkedScoreData);
    } catch (ScoringFilterException e) {
      LOG.warn("Scoring filters failed with exception " +
                StringUtils.stringifyException(e));
    }

    // clear markers

    page.removeFromMetadata(FetcherJob.REDIRECT_DISCOVERED);
    Mark.GENERATE_MARK.removeMark(page);
    Mark.FETCH_MARK.removeMark(page);
    Utf8 mark = Mark.PARSE_MARK.removeMark(page);
    if (mark != null) {
      Mark.UPDATEDB_MARK.putMark(page, mark);
    }

    context.write(key, page);
  }

}

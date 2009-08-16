package org.apache.nutch.crawl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.SignatureComparator;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.parse.TableParser;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.hbase.WebTableColumns;
import org.apache.nutch.util.hbase.TableUtil;
import org.apache.nutch.util.hbase.WebTableRow;

public class TableUpdateReducer
extends TableReducer<ImmutableBytesWritable, NutchWritable, ImmutableBytesWritable> {
  
  public static final Log LOG = TableUpdater.LOG;

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
    additionsAllowed = conf.getBoolean(CrawlDb.CRAWLDB_ADDITIONS_ALLOWED, true);
    maxInterval = conf.getInt("db.fetch.interval.max", 0 );
    schedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
  }
  
  @Override
  protected void reduce(ImmutableBytesWritable key, Iterable<NutchWritable> values,
      Context context) throws IOException, InterruptedException {

    WebTableRow row = null;
    inlinkedScoreData.clear();

    for (NutchWritable nutchWritable : values) {
      Writable val = nutchWritable.get();
      if (val instanceof WebTableRow) {
        row = (WebTableRow) val;
      } else {
        inlinkedScoreData.add((ScoreDatum) val);
      }
    }
    String url;
    try {
      url = TableUtil.unreverseUrl(Bytes.toString(key.get()));
    } catch (Exception e) {
      // this can happen because a newly discovered malformed link
      // may slip by url filters
      // TODO: Find a better solution
      return;
    }

    if (row == null) { // new row
      if (!additionsAllowed) {
        return;
      }
      row = new WebTableRow(key.get());
      schedule.initializeSchedule(url, row);
      row.setStatus(CrawlDatumHbase.STATUS_UNFETCHED);
      try {
        scoringFilters.initialScore(url, row);
      } catch (ScoringFilterException e) {
        row.setScore(0.0f);
      }
    } else {
      if (row.hasMeta(Fetcher.REDIRECT_DISCOVERED) && !row.hasColumn(WebTableColumns.STATUS, null)) {
        // this row is marked during fetch as the destination of a redirect
        // but does not contain anything else, so we initialize it.
        schedule.initializeSchedule(url, row);
        row.setStatus(CrawlDatumHbase.STATUS_UNFETCHED);
        try {
          scoringFilters.initialScore(url, row);
        } catch (ScoringFilterException e) {
          row.setScore(0.0f);
        }
      } else { // update row
        byte status = row.getStatus();
        switch (status) {
        case CrawlDatumHbase.STATUS_FETCHED:         // succesful fetch
        case CrawlDatumHbase.STATUS_REDIR_TEMP:      // successful fetch, redirected
        case CrawlDatumHbase.STATUS_REDIR_PERM:
        case CrawlDatumHbase.STATUS_NOTMODIFIED:     // successful fetch, notmodified
          int modified = FetchSchedule.STATUS_UNKNOWN;
          if (status == CrawlDatumHbase.STATUS_NOTMODIFIED) {
            modified = FetchSchedule.STATUS_NOTMODIFIED;
          }
          byte[] prevSig = row.getPrevSignature();
          byte[] signature = row.getSignature();
          if (prevSig != null && signature != null) {
            if (SignatureComparator.compare(prevSig, signature) != 0) {
              modified = FetchSchedule.STATUS_MODIFIED;
            } else {
              modified = FetchSchedule.STATUS_NOTMODIFIED;
            }
          }
          long fetchTime = row.getFetchTime();
          long prevFetchTime = row.getPrevFetchTime();
          long modifiedTime = row.getModifiedTime();

          schedule.setFetchSchedule(url, row, prevFetchTime, 0L,
              fetchTime, modifiedTime, modified);
          if (maxInterval < row.getFetchInterval())
            schedule.forceRefetch(url, row, false);
          break;
        case CrawlDatumHbase.STATUS_RETRY:
          schedule.setPageRetrySchedule(url, row, 0L, 0L, row.getFetchTime());
          if (row.getRetriesSinceFetch() < retryMax) {
            row.setStatus(CrawlDatumHbase.STATUS_UNFETCHED);
          } else {
            row.setStatus(CrawlDatumHbase.STATUS_GONE);
          }
          break;
        case CrawlDatumHbase.STATUS_GONE:
          schedule.setPageGoneSchedule(url, row, 0L, 0L, row.getFetchTime());
          break;
        }
      }
    }

    row.deleteAllInlinks();
    for (ScoreDatum inlink : inlinkedScoreData) {
      row.addInlink(new Inlink(inlink.getUrl(), inlink.getAnchor()));
    }
    
    try {
      scoringFilters.updateScore(url, row, inlinkedScoreData);
    } catch (ScoringFilterException e) {
      LOG.warn("Scoring filters failed with exception " +
                StringUtils.stringifyException(e));
    }

    // clear markers
    row.deleteMeta(Fetcher.REDIRECT_DISCOVERED);
    row.deleteMeta(Generator.GENERATOR_MARK);
    row.deleteMeta(Fetcher.FETCH_MARK);
    row.deleteMeta(TableParser.PARSE_MARK);

    row.makeRowMutation(key.get()).writeToContext(key, context);
  }

}

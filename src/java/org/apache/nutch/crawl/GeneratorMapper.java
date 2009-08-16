package org.apache.nutch.crawl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.crawl.Generator.SelectorEntry;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.hbase.WebTableRow;
import org.apache.nutch.util.hbase.TableUtil;

public class GeneratorMapper 
extends TableMapper<SelectorEntry, WebTableRow> {

  private URLFilters filters;
  private URLNormalizers normalizers;
  private boolean filter;
  private FetchSchedule schedule;
  private ScoringFilters scoringFilters;
  private long curTime;

  @Override
  public void map(ImmutableBytesWritable key, Result result,
      Context context) throws IOException, InterruptedException {
    String reversedUrl = Bytes.toString(key.get());
    String url = TableUtil.unreverseUrl(reversedUrl);

    WebTableRow row = new WebTableRow(result);

    // If filtering is on don't generate URLs that don't pass URLFilters
    try {
      url = normalizers.normalize(url, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
      if (filter && filters.filter(url) == null)
        return;
    } catch (URLFilterException e) {
      Generator.LOG.warn("Couldn't filter url: " + url + " (" + e.getMessage() + ")");
      return;
    }

    // check fetch schedule
    if (!schedule.shouldFetch(url, row, curTime)) {
      if (Generator.LOG.isDebugEnabled()) {
        Generator.LOG.debug("-shouldFetch rejected '" + url + "', fetchTime=" + 
            row.getFetchTime() + ", curTime=" + curTime);
      }
      return;
    }
    
    float score = row.getScore();
    try {
      score = scoringFilters.generatorSortValue(url, row, score);
    } catch (ScoringFilterException e) { 
      // ignore
    }
    SelectorEntry entry = new SelectorEntry(url, score);
    context.write(entry, row);

  }

  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    filters = new URLFilters(conf);
    curTime = conf.getLong(Generator.CRAWL_GEN_CUR_TIME,
        System.currentTimeMillis());
    normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
    filter = conf.getBoolean(Generator.CRAWL_GENERATE_FILTER, true);
    schedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
  }
}

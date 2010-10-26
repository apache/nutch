package org.apache.nutch.crawl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.GeneratorJob.SelectorEntry;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.apache.gora.mapreduce.GoraMapper;

public class GeneratorMapper
extends GoraMapper<String, WebPage, SelectorEntry, WebPage> {

  private URLFilters filters;
  private URLNormalizers normalizers;
  private boolean filter;
  private boolean normalise;
  private FetchSchedule schedule;
  private ScoringFilters scoringFilters;
  private long curTime;

  @Override
  public void map(String reversedUrl, WebPage page,
      Context context) throws IOException, InterruptedException {
    String url = TableUtil.unreverseUrl(reversedUrl);

    if (Mark.GENERATE_MARK.checkMark(page) != null) {
      if (GeneratorJob.LOG.isDebugEnabled()) {
        GeneratorJob.LOG.debug("Skipping " + url + "; already generated");
      }
    }

    // If filtering is on don't generate URLs that don't pass URLFilters
    try {
      if (normalise) {
        url = normalizers.normalize(url, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
      }
      if (filter && filters.filter(url) == null)
        return;
    } catch (URLFilterException e) {
      GeneratorJob.LOG.warn("Couldn't filter url: " + url + " (" + e.getMessage() + ")");
      return;
    }

    // check fetch schedule
    if (!schedule.shouldFetch(url, page, curTime)) {
      if (GeneratorJob.LOG.isDebugEnabled()) {
        GeneratorJob.LOG.debug("-shouldFetch rejected '" + url + "', fetchTime=" +
            page.getFetchTime() + ", curTime=" + curTime);
      }
      return;
    }
    float score = page.getScore();
    try {
      score = scoringFilters.generatorSortValue(url, page, score);
    } catch (ScoringFilterException e) {
      //ignore
    }
    SelectorEntry entry = new SelectorEntry(url, score);
    context.write(entry, page);
  }

  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    filters = new URLFilters(conf);
    curTime =
      conf.getLong(GeneratorJob.GENERATOR_CUR_TIME, System.currentTimeMillis());
    normalizers =
      new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
    filter = conf.getBoolean(GeneratorJob.GENERATOR_FILTER, true);
    normalise = conf.getBoolean(GeneratorJob.GENERATOR_NORMALISE, true);
    schedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
  }
}

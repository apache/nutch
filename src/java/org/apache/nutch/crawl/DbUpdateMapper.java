package org.apache.nutch.crawl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.WebPageWritable;
import org.gora.mapreduce.GoraMapper;

public class DbUpdateMapper
extends GoraMapper<String, WebPage, String, NutchWritable> {
  public static final Log LOG = DbUpdaterJob.LOG;

  private ScoringFilters scoringFilters;

  private final List<ScoreDatum> scoreData = new ArrayList<ScoreDatum>();

  @Override
  public void map(String key, WebPage page, Context context)
  throws IOException, InterruptedException {

    String url = TableUtil.unreverseUrl(key);

    scoreData.clear();
    Map<Utf8, Utf8> outlinks = page.getOutlinks();
    if (outlinks != null) {
      for (Entry<Utf8, Utf8> e : outlinks.entrySet()) {
        scoreData.add(new ScoreDatum(0.0f, e.getKey().toString(), e.getValue().toString()));
      }
    }

    // TODO: Outlink filtering (i.e. "only keep the first n outlinks")
    try {
      scoringFilters.distributeScoreToOutlinks(url, page, scoreData, (outlinks == null ? 0 : outlinks.size()));
    } catch (ScoringFilterException e) {
      LOG.warn("Distributing score failed for URL: " + key +
          " exception:" + StringUtils.stringifyException(e));
    }

    context.write(key,
        new NutchWritable(new WebPageWritable(context.getConfiguration(), page)));

    for (ScoreDatum scoreDatum : scoreData) {
      String reversedOut = TableUtil.reverseUrl(scoreDatum.getUrl());
      scoreDatum.setUrl(url);
      context.write(reversedOut, new NutchWritable(scoreDatum));
    }
  }

  @Override
  public void setup(Context context) {
    scoringFilters = new ScoringFilters(context.getConfiguration());
  }

}

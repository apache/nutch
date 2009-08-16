package org.apache.nutch.crawl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.hbase.WebTableRow;
import org.apache.nutch.util.hbase.TableUtil;

public class TableUpdateMapper
extends TableMapper<ImmutableBytesWritable, NutchWritable> {
  public static final Log LOG = TableUpdater.LOG;

  private ScoringFilters scoringFilters;
  
  private List<ScoreDatum> scoreData = new ArrayList<ScoreDatum>();

  @Override
  public void map(ImmutableBytesWritable key, Result result, Context context)
  throws IOException, InterruptedException {

    String url = TableUtil.unreverseUrl(Bytes.toString(key.get()));
    WebTableRow row = new WebTableRow(result);

    Collection<Outlink> outlinks = row.getOutlinks();

    scoreData.clear();
    for (Outlink outlink : outlinks) {
      scoreData.add(new ScoreDatum(0.0f, outlink.getToUrl(), outlink.getAnchor()));
    }

    // TODO: Outlink filtering (i.e. "only keep the first n outlinks")
    try {
      scoringFilters.distributeScoreToOutlinks(url, row, scoreData, outlinks.size());
    } catch (ScoringFilterException e) {
      LOG.warn("Distributing score failed for URL: " + key +
          " exception:" + StringUtils.stringifyException(e));
    }

    context.write(key, new NutchWritable(row));

    for (ScoreDatum scoreDatum : scoreData) {
      String reversedOut = TableUtil.reverseUrl(scoreDatum.getUrl());
      ImmutableBytesWritable outKey =
        new ImmutableBytesWritable(reversedOut.getBytes());
      scoreDatum.setUrl(url);
      context.write(outKey, new NutchWritable(scoreDatum));
    }
  }

  @Override
  public void setup(Context context) {
    scoringFilters = new ScoringFilters(context.getConfiguration());
  }

}

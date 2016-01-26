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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.util.Utf8;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.Mark;
import org.slf4j.Logger;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.WebPageWritable;
import org.apache.gora.mapreduce.GoraMapper;

public class DbUpdateMapper extends
GoraMapper<String, WebPage, UrlWithScore, NutchWritable> {
  public static final Logger LOG = DbUpdaterJob.LOG;

  private ScoringFilters scoringFilters;

  private final List<ScoreDatum> scoreData = new ArrayList<ScoreDatum>();

  @SuppressWarnings("unused")
  private Utf8 batchId;

  // reuse writables
  private UrlWithScore urlWithScore = new UrlWithScore();
  private NutchWritable nutchWritable = new NutchWritable();
  private WebPageWritable pageWritable;

  @Override
  public void map(String key, WebPage page, Context context)
      throws IOException, InterruptedException {
    if (Mark.GENERATE_MARK.checkMark(page) == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping " + TableUtil.unreverseUrl(key)
        + "; not generated yet");
      }
      return;
    }

    String url = TableUtil.unreverseUrl(key);

    scoreData.clear();
    Map<CharSequence, CharSequence> outlinks = page.getOutlinks();
    addScoreData(page, outlinks);

    Map<CharSequence, CharSequence> sitemaps = page.getSitemaps();
    addScoreData(page, sitemaps);

    // TODO: Outlink filtering (i.e. "only keep the first n outlinks")
    try {
      scoringFilters.distributeScoreToOutlinks(url, page, scoreData,
          (outlinks == null ? 0 : outlinks.size()));
    } catch (ScoringFilterException e) {
      LOG.warn("Distributing score failed for URL: " + key + " exception:"
          + StringUtils.stringifyException(e));
    }

    urlWithScore.setUrl(key);
    urlWithScore.setScore(Float.MAX_VALUE);
    pageWritable.setWebPage(page);
    nutchWritable.set(pageWritable);
    context.write(urlWithScore, nutchWritable);

    for (ScoreDatum scoreDatum : scoreData) {
      String reversedOut = TableUtil.reverseUrl(scoreDatum.getUrl());
      scoreDatum.setUrl(url);
      urlWithScore.setUrl(reversedOut);
      urlWithScore.setScore(scoreDatum.getScore());
      nutchWritable.set(scoreDatum);
      context.write(urlWithScore, nutchWritable);
    }
  }

  private void addScoreData(WebPage page, Map<CharSequence, CharSequence> map) {
    if (map != null) {
      for (Entry<CharSequence, CharSequence> e : map.entrySet()) {
        int depth = Integer.MAX_VALUE;
        CharSequence depthUtf8 = page.getMarkers().get(DbUpdaterJob.DISTANCE);
        if (depthUtf8 != null)
          depth = Integer.parseInt(depthUtf8.toString());
        scoreData.add(new ScoreDatum(0.0f, e.getKey().toString(), e.getValue()
            .toString(), depth));
      }
    }
  }

  @Override
  public void setup(Context context) {
    scoringFilters = new ScoringFilters(context.getConfiguration());
    pageWritable = new WebPageWritable(context.getConfiguration(), null);
    batchId = new Utf8(context.getConfiguration().get(Nutch.BATCH_NAME_KEY,
        Nutch.ALL_BATCH_ID_STR));
  }

}

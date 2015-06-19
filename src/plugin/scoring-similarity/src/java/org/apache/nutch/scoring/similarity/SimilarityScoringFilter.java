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
package org.apache.nutch.scoring.similarity;

import java.util.Collection;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.AbstractScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.similarity.cosine.CosineSimilarityModel;

public class SimilarityScoringFilter extends AbstractScoringFilter {

  private Configuration conf;
  private SimilarityModel similarityModel;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    similarityModel = new CosineSimilarityModel();
    similarityModel.setConf(conf);
  }

  @Override
  public void passScoreAfterParsing(Text url, Content content, Parse parse)
      throws ScoringFilterException {

    float score = similarityModel.setURLScoreAfterParsing(url, content, parse);
    parse.getData().getContentMeta()
    .set(Nutch.SCORE_KEY, score+"");
  }

  @Override
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl,
      ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
      CrawlDatum adjust, int allCount) throws ScoringFilterException {
    similarityModel.distributeScoreToOutlinks(fromUrl, parseData, targets, adjust, allCount);
    return adjust;
  }
}

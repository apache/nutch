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
package org.apache.nutch.scoring.similarity.cosine;

import java.lang.invoke.MethodHandles;
import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.similarity.SimilarityModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CosineSimilarity implements SimilarityModel{

  private Configuration conf;
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public float setURLScoreAfterParsing(Text url, Content content, Parse parse) {
    float score = 1;

    try {
      if(!Model.isModelCreated){
        Model.createModel(conf);
      }
      String metatags = parse.getData().getParseMeta().get("metatag.keyword");
      String metaDescription = parse.getData().getParseMeta().get("metatag.description");
      int[] ngramArr = Model.retrieveNgrams(conf);
      int mingram = ngramArr[0];
      int maxgram = ngramArr[1];
      DocVector docVector = Model.createDocVector(parse.getText()+metaDescription+metatags, mingram, maxgram);
      if(docVector!=null){
        score = Model.computeCosineSimilarity(docVector);
        LOG.info("Setting score of {} to {}",url, score);
      }
      else {
        throw new Exception("Could not create DocVector from parsed text");
      }
    } catch (Exception e) {
      LOG.error("Error creating Cosine Model, setting scores of urls to 1 : {}", StringUtils.stringifyException(e));
    }
    return score;
  }

  @Override
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl, ParseData parseData,
      Collection<Entry<Text, CrawlDatum>> targets, CrawlDatum adjust,
      int allCount) {
    float score = Float.parseFloat(parseData.getContentMeta().get(Nutch.SCORE_KEY));
    for (Entry<Text, CrawlDatum> target : targets) {
      target.getValue().setScore(score);
    }
    return adjust;
  }

}

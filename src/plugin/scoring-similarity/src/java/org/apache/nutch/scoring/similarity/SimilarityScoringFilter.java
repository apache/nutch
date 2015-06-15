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

import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.AbstractScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimilarityScoringFilter extends AbstractScoringFilter {

  private Configuration conf;
  private String goldStandardDocPath;
  private static DocumentVector goldStandardDocVect;
  private final static Logger LOG = LoggerFactory
      .getLogger(SimilarityScoringFilter.class);

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    goldStandardDocPath = conf.get("similarity.model.path");
    Reader reader = conf.getConfResourceAsReader(goldStandardDocPath);
    try {
      String fileContent = IOUtils.toString(reader);
      goldStandardDocVect = new DocumentVector(fileContent, conf);
      LOG.info("Creating DocVector from path - {}",goldStandardDocPath);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }    
  }

  @Override
  public void passScoreAfterParsing(Text url, Content content, Parse parse)
      throws ScoringFilterException {
    CosineSimilarity cs = new CosineSimilarity(conf);
    
    DocumentVector parseTextDocVect = new DocumentVector(parse.getText(), conf);
    double parseTextSimilarity = cs.calculateCosineSimilarity(goldStandardDocVect, parseTextDocVect);
  
    DocumentVector parseMetaKeywordDocVect = new DocumentVector(parse.getData().getParseMeta().get("metatag.keyword"), conf);
    double metaKeywordSimilarity = cs.calculateCosineSimilarity(goldStandardDocVect, parseMetaKeywordDocVect);
    
    DocumentVector parseMetaDescDocVect = new DocumentVector(parse.getData().getParseMeta().get("metatag.description"), conf);
    double metaDescriptionSimilarity = cs.calculateCosineSimilarity(goldStandardDocVect, parseMetaDescDocVect);
    
    LOG.info("Calculating similarity between gold-standard and {}",url);
    int count = 0;
    if(parseTextSimilarity!=0)
      count++;
    if(metaDescriptionSimilarity!=0)
      count++;
    if(metaKeywordSimilarity!=0)
      count++;
    if(count==0)
      count++;

    double score =  (parseTextSimilarity+metaDescriptionSimilarity + metaKeywordSimilarity)/count;
    LOG.info("Setting score of {} to {}",url, score);
    LOG.info("Score break down TextSimilarity : {}, metaKeywordSimilarity : {}, metaDescriptionSimilarity : {}",
        parseTextSimilarity, metaKeywordSimilarity, metaDescriptionSimilarity);
    parse.getData().getContentMeta()
    .set(Nutch.SCORE_KEY, score+"");
  }

  @Override
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl,
      ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
      CrawlDatum adjust, int allCount) throws ScoringFilterException {
    float score = Float.parseFloat(parseData.getContentMeta().get(Nutch.SCORE_KEY));
    for (Entry<Text, CrawlDatum> target : targets) {
      target.getValue().setScore((float)score);
      LOG.info("Setting score of {} to {}",target.getKey(), score);
    }
    return adjust;
  }
}

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
package org.apache.nutch.scoring.similarity.Cosine;

import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.similarity.SimilarityModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CosineSimilarityModel implements SimilarityModel{

  private Configuration conf;
  private String goldStandardDocPath;
  private static DocumentVector goldStandardDocVect;
  private final static Logger LOG = LoggerFactory
      .getLogger(CosineSimilarityModel.class);
  
  private double calculateCosineSimilarity(String goldStandard, String document2){

    DocumentVector docVect1 = new DocumentVector(goldStandard, conf);
    DocumentVector docVect2 = new DocumentVector(document2, conf);

    double doc1Dist = getEuclideanDist(docVect1);
    double doc2Dist = getEuclideanDist(docVect2);

    double dotProduct = getDotProduct(docVect1, docVect2);
    if(doc1Dist*doc2Dist == 0){
      return 0.0;
    }
    return dotProduct/(doc1Dist*doc2Dist);
  }

  private double calculateCosineSimilarity(DocumentVector docVect1, DocumentVector docVect2){

    double doc1Dist = getEuclideanDist(docVect1);
    double doc2Dist = getEuclideanDist(docVect2);

    double dotProduct = getDotProduct(docVect1, docVect2);
    if(doc1Dist*doc2Dist == 0){
      return 0.0;
    }
    return dotProduct/(doc1Dist*doc2Dist);
  }

  private double getDotProduct(DocumentVector docVect1, DocumentVector docVect2) {
    double dotProduct = 0.0;
    Map<String, Integer> doc2TermFreqVect = docVect2.getTermFreqVect();
    for(Map.Entry<String, Integer> pair : docVect1.getTermFreqVect().entrySet()){
      double doc1value = pair.getValue();
      double doc2value = 0;

      if(doc2TermFreqVect.containsKey(pair.getKey()))
        doc2value = doc2TermFreqVect.get(pair.getKey());

      dotProduct += doc1value*doc2value;
    }

    return dotProduct;
  }

  private double getEuclideanDist(DocumentVector docVect) {
    float sum = 0f;
    for(Map.Entry<String, Integer> pair : docVect.getTermFreqVect().entrySet()){
      sum += pair.getValue() * pair.getValue();
    }    
    return Math.sqrt(sum);
  }
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    goldStandardDocPath = conf.get("scoring.similarity.model.path");
    Reader reader = conf.getConfResourceAsReader(goldStandardDocPath);
    try {
      String fileContent = IOUtils.toString(reader);
      if(goldStandardDocVect == null) {
        goldStandardDocVect = new DocumentVector(fileContent, conf);
        LOG.info("Creating DocVector from path - {}",goldStandardDocPath);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }    
  }
  @Override
  public float setURLScoreAfterParsing(Text url, Content content, Parse parse) {

    DocumentVector parseTextDocVect = new DocumentVector(parse.getText(), conf);
    double parseTextSimilarity = calculateCosineSimilarity(goldStandardDocVect, parseTextDocVect);
  
    DocumentVector parseMetaKeywordDocVect = new DocumentVector(parse.getData().getParseMeta().get("metatag.keyword"), conf);
    double metaKeywordSimilarity = calculateCosineSimilarity(goldStandardDocVect, parseMetaKeywordDocVect);
    
    DocumentVector parseMetaDescDocVect = new DocumentVector(parse.getData().getParseMeta().get("metatag.description"), conf);
    double metaDescriptionSimilarity = calculateCosineSimilarity(goldStandardDocVect, parseMetaDescDocVect);
    
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

    float score =  (float) ((parseTextSimilarity+metaDescriptionSimilarity + metaKeywordSimilarity)/count);
    LOG.info("Setting score of {} to {}",url, score);
    LOG.info("Score break down TextSimilarity : {}, metaKeywordSimilarity : {}, metaDescriptionSimilarity : {}",
        parseTextSimilarity, metaKeywordSimilarity, metaDescriptionSimilarity);
    return score;
  }
  
  @Override
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl,
      ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
      CrawlDatum adjust, int allCount) {
    float score = Float.parseFloat(parseData.getContentMeta().get(Nutch.SCORE_KEY));
    for (Entry<Text, CrawlDatum> target : targets) {
      target.getValue().setScore((float)score);
      LOG.info("Setting score of {} to {}",target.getKey(), score);
    }
    return adjust;
  }
}

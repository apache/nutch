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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class CosineSimilarity {

  private Configuration conf;
  
  public CosineSimilarity(Configuration conf){
    this.conf = conf;
  }
  public double calculateCosineSimilarity(String goldStandard, String document2){

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

  public double calculateCosineSimilarity(DocumentVector docVect1, DocumentVector docVect2){

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
}

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

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.scoring.similarity.cosine.DocumentVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentVector {

  private Map<String, Integer> termFreqVect;
  private static Set<String> stopwordSet;
  private final static Logger LOG = LoggerFactory
      .getLogger(DocumentVector.class);

  public DocumentVector(String text, Configuration conf){

    termFreqVect = new HashMap<String, Integer>();
    if(stopwordSet == null){
      stopwordSet = new HashSet<String>();
      populateStopWordSet(conf);
    }
    createDocVect(text);
    removeStopWords(conf);
  }

  private void createDocVect(String text){
    if(text!=null){
      String[] tokens = text.replaceAll("[^a-zA-Z0-9 ]", " ").toLowerCase().split("\\s+");
      for(String token: tokens){
        if(termFreqVect.containsKey(token)){
          int count = termFreqVect.get(token)+1;
          termFreqVect.put(token, count);
        }
        else
          termFreqVect.put(token, 1);
      }
    }
  }

  public Map<String, Integer> getTermFreqVect(){
    return termFreqVect;
  }

  private void removeStopWords(Configuration conf){
    for(String stopWord: stopwordSet){
      stopWord = stopWord.trim();
      if(termFreqVect.containsKey(stopWord)){
        termFreqVect.remove(stopWord);
      }
    }
  }

  private void populateStopWordSet(Configuration conf){
    String stopWordFilePath = conf.get("scoring.similarity.stopword.file");
    Reader reader = conf.getConfResourceAsReader(stopWordFilePath);
    try {
      LOG.info("Populating stopwords from {}", stopWordFilePath);
      String[] stopWordList1 = IOUtils.toString(reader).split("\n");
      for(String stopWord: stopWordList1){
        stopWord = stopWord.trim();
        stopwordSet.add(stopWord);
      }
    } catch (IOException e) {
      LOG.error("Failed to populate stopwords : {}", StringUtils.stringifyException(e));
      e.printStackTrace();
    }
  }
}

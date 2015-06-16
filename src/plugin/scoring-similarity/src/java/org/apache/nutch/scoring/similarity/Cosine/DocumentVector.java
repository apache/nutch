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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

public class DocumentVector {

  Map<String, Integer> termFreqVect;

  public DocumentVector(String text, Configuration conf){
    
    termFreqVect = new HashMap<String, Integer>();
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
    String stopWordFilePath = conf.get("scoring.similarity.stopword.file");
    Reader reader = conf.getConfResourceAsReader(stopWordFilePath);
    try {
      String[] stopWordList1 = IOUtils.toString(reader).split("\n");
      for(String stopWord: stopWordList1){
        stopWord = stopWord.trim();
        if(termFreqVect.containsKey(stopWord)){
          termFreqVect.remove(stopWord);
        }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}

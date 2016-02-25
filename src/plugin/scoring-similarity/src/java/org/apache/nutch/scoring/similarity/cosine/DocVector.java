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

import java.util.HashMap;
import java.util.Map;

public class DocVector {

  public HashMap<Integer, Long> termVector;
  public HashMap<String, Integer> termFreqVector;

  public DocVector() {
    termFreqVector = new HashMap<>();
  }

  public void setTermFreqVector(HashMap<String, Integer> termFreqVector) {
    this.termFreqVector = termFreqVector;
  }
  
  public void setVectorEntry(int pos, long freq) {
    termVector.put(pos, freq);
  }
  
  public float dotProduct(DocVector docVector) {
    float product = 0;
    for(Map.Entry<String, Integer> entry : termFreqVector.entrySet()) {
      if(docVector.termFreqVector.containsKey(entry.getKey())) {
        product += docVector.termFreqVector.get(entry.getKey())*entry.getValue();
      }
    }
    return product;
  }
  
  public float getL2Norm() {
    float sum = 0;
    for(Map.Entry<String, Integer> entry : termFreqVector.entrySet()) {
      sum += entry.getValue()*entry.getValue();
    }
    return (float) Math.sqrt(sum);
  }

}

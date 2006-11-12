/*
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
package org.apache.nutch.clustering;

import java.util.ArrayList;
import java.util.List;

import org.apache.nutch.webapp.common.SearchResultBean;

/** Wrapper for HitsCluster
 * 
 */
public class ClusterResult {

  private int numDocs;
  private String labelString="";
  HitsCluster cluster;
  
  public ClusterResult(HitsCluster cluster, int numLabels, int numDocs){
    this.numDocs=numDocs;
    this.cluster=cluster;
    int maxSize=Math.min(numLabels, cluster.getDescriptionLabels().length);
    for(int i=0;i<maxSize;i++){
      if(i>0) {
        labelString+=", ";
      }
      labelString+=cluster.getDescriptionLabels()[i];
    }
  }
  
  public String getLabel(){
    return labelString;
  }  
  
  /** return document samples of cluster
   * 
   * @return
   */
  public List getDocs(){
    int maxNumDocs=Math.min(numDocs,cluster.getHits().length);
    List docs=new ArrayList(maxNumDocs);
    for(int i=0;i<maxNumDocs;i++){
      docs.add(new SearchResultBean(null, null, cluster.getHits()[i],null));
    }
    return docs;
  }

}

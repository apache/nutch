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

/**
 * Wrapper object to clustering results
 */
public class Clusters {
  
  private List clusters;

  public Clusters(final HitsCluster [] clustersArray, final int numClusters, final int numDocs, final int numLabels){
    clusters=new ArrayList();
    for(int i=0;i<numClusters;i++){
      clusters.add(new ClusterResult(clustersArray[i], numDocs, numLabels));
    }
  }
  
  public List getClusters(){
    return clusters;
  }
  
  public boolean getHasClusters(){
    return clusters.size()>0;
  }


}

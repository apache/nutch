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
package org.apache.nutch.clustering.carrot2;

import java.util.Iterator;
import java.util.List;

import org.apache.nutch.clustering.HitsCluster;
import org.apache.nutch.searcher.HitDetails;
import org.carrot2.core.clustering.RawCluster;
import org.carrot2.core.clustering.RawDocument;

/**
 * An adapter of Carrot2's {@link RawCluster} interface to
 * {@link HitsCluster} interface. 
 */
public class HitsClusterAdapter implements HitsCluster {
  private RawCluster rawCluster;
  private HitDetails [] hits;

  /**
   * Lazily initialized subclusters array.
   */
  private HitsCluster [] subclusters;
  
  /**
   * Lazily initialized documents array.
   */
  private HitDetails [] documents;
  
  /**
   * Creates a new adapter.
   */
  public HitsClusterAdapter(RawCluster rawCluster, HitDetails [] hits) {
    this.rawCluster = rawCluster;
    this.hits = hits;
  }

  /*
   * @see org.apache.nutch.clustering.HitsCluster#getSubclusters()
   */
  public HitsCluster[] getSubclusters() {
    if (this.subclusters == null) {
      final List rawSubclusters = rawCluster.getSubclusters();
      if (rawSubclusters == null || rawSubclusters.size() == 0) {
        subclusters = null;
      } else {
        subclusters = new HitsCluster[rawSubclusters.size()];
        int j = 0;
        for (Iterator i = rawSubclusters.iterator(); i.hasNext(); j++) {
          RawCluster c = (RawCluster) i.next();
          subclusters[j] = new HitsClusterAdapter(c, hits);
        }
      }
    }

    return subclusters;
  }

  /*
   * @see org.apache.nutch.clustering.HitsCluster#getHits()
   */
  public HitDetails[] getHits() {
    if (documents == null) {
      List rawDocuments = this.rawCluster.getDocuments();
      documents = new HitDetails[ rawDocuments.size() ];
      
      int j = 0;
      for (Iterator i = rawDocuments.iterator(); i.hasNext(); j++) {
        RawDocument doc = (RawDocument) i.next();
        Integer offset = (Integer) doc.getId();
        documents[j] = this.hits[offset.intValue()];
      }
    }

    return documents;
  }

  /*
   * @see org.apache.nutch.clustering.HitsCluster#getDescriptionLabels()
   */
  public String[] getDescriptionLabels() {
    List phrases = this.rawCluster.getClusterDescription();
    return (String []) phrases.toArray( new String [ phrases.size() ]);
  }

  /*
   * @see org.apache.nutch.clustering.HitsCluster#isJunkCluster()
   */
  public boolean isJunkCluster() {
    return rawCluster.getProperty(RawCluster.PROPERTY_JUNK_CLUSTER) != null;
  }
}


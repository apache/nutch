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

package org.apache.nutch.clustering;

import org.apache.nutch.searcher.HitDetails;

/**
 * An interface representing a group (cluster) of related hits.
 * 
 * <p>If {@link #isJunkCluster()} method returns <code>true</code>
 * then this cluster contains documents that are grouped together,
 * but no clear semantic relation has been detected. Such clusters may
 * be hidden in the user interface layer, unless someone wishes to
 * see an explicit group of documents that didn't belong anywhere else.</p>
 *
 * @author Dawid Weiss
 * @version $Id: HitsCluster.java,v 1.1 2004/08/09 23:23:52 johnnx Exp $
 */
public interface HitsCluster {
  /**
   * @return Returns an array of {@link HitsCluster} objects
   * that are sub-groups of the current group, or <code>null</code>
   * if this cluster has no sub-groups.
   */
  public HitsCluster [] getSubclusters();
  
  /**
   * @return Returns a relevance-ordered array of the hits belonging
   * to this cluster or <code>null</code> if this cluster
   * has no associated documents (it may have subclusters only).
   */
  public HitDetails[] getHits();
  
  /**
   * @return Returns an array of labels for this cluster. The labels should
   * be sorted according to their relevance to the cluster's content. Not
   * all of the labels must be displayed - the application is free to
   * set a cutoff threshold and display only the topmost labels. 
   */
  public String[] getDescriptionLabels();

  /**
   * Returns <code>true</code> if this cluster constains documents
   * that did not fit anywhere else (presentation layer may
   * discard such clusters). 
   * 
   * <p>Subclusters of this cluster are also junk clusters, even if
   * they don't have this property set to <code>true</code></p>
   */
  public boolean isJunkCluster();
}

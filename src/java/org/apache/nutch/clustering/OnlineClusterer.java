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

// Nutch imports
import org.apache.nutch.plugin.Pluggable;
import org.apache.nutch.searcher.HitDetails;


/**
 * An extension point interface for online search results clustering
 * algorithms.
 *
 * <p>By the term <b>online</b> search results clustering we will understand
 * a clusterer that works on a set of {@link HitDetails} retrieved for a
 * query and able to produce a set of {@link HitsCluster} that can be displayed
 * to help the user gain more insight in the topics found in the result.</p>
 *
 * <p>Other clustering options include predefined categories and off-line
 * preclustered groups, but I do not investigate those any further here.</p>
 *
 * @author Dawid Weiss
 * @version $Id: OnlineClusterer.java,v 1.1 2004/08/09 23:23:52 johnnx Exp $
 */
public interface OnlineClusterer extends Pluggable {
  /** The name of the extension point. */
  public final static String X_POINT_ID = OnlineClusterer.class.getName();

  /**
   * Clusters an array of hits ({@link HitDetails} objects) and
   * their previously extracted summaries (<code>String</code>s).
   * 
   * <p>Arguments to this method may seem to be very low-level, but
   * in fact they are side products of a regular search process, 
   * so we simply reuse them instead of duplicating part of the usual
   * Nutch functionality. Other ideas are welcome.</p>
   * 
   * <p>This method must be thread-safe (many threads may invoke
   * it concurrently on the same instance of a clusterer).</p>
   * 
   * @return A set of {@link HitsCluster} objects.
   */
  public HitsCluster [] clusterHits(HitDetails [] hitDetails, String [] descriptions);
}

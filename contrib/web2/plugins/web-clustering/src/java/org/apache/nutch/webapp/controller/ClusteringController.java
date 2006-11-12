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
package org.apache.nutch.webapp.controller;

import java.io.IOException;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nutch.clustering.ClusteringPresearchExtension;
import org.apache.nutch.clustering.Clusters;
import org.apache.nutch.clustering.HitsCluster;
import org.apache.nutch.clustering.OnlineClusterer;
import org.apache.nutch.clustering.OnlineClustererFactory;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.Summary;
import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.nutch.webapp.common.Startable;
import org.apache.struts.tiles.ComponentContext;

public class ClusteringController extends NutchController implements Startable {

  public static final String REQ_ATTR_CLUSTERS = "clusters";

  static OnlineClusterer clusterer = null;

  public void nutchPerform(ComponentContext tileContext,
      HttpServletRequest request, HttpServletResponse response,
      ServletContext servletContext) throws ServletException, IOException {

    ServiceLocator locator = getServiceLocator(request);

    if (ClusteringPresearchExtension.isClusteringActive(locator)) {

      // display top N clusters and top Q documents inside them.
      int N = locator.getConfiguration().getInt(
          "extension.clustering.cluster-count", 10);
      int Q = locator.getConfiguration().getInt(
          "extension.clustering.cluster-top-documents-count", 3);
      int maxLabels = 2;

      HitDetails[] details = locator.getSearch().getDetails();
      Summary[] summaries = locator.getSearch().getSummaries();

      HitsCluster[] clusters = null;
      if (clusterer != null) {
        final long clusteringStart = System.currentTimeMillis();
        try {
          clusters = clusterer.clusterHits(details, Summary
              .toStrings(summaries));
          final long clusteringDuration = System.currentTimeMillis()
              - clusteringStart;
          LOG.info("Clustering took: " + clusteringDuration + " milliseconds.");

        } catch (Exception e) {
          LOG.info("Could not do clustering???" + e);
          return;
        }
      }

      // set new limit if fever than N results
      N = Math.min(N, clusters.length);

      // set to request
      Clusters clusterResult = new Clusters(clusters, N, Q, maxLabels);
      request.setAttribute(REQ_ATTR_CLUSTERS, clusterResult);
    }
  }

  public void start(ServletContext servletContext) {
    ServiceLocator locator = getServiceLocator(servletContext);
    try {
      clusterer = new OnlineClustererFactory(locator.getConfiguration())
          .getOnlineClusterer();
    } catch (PluginRuntimeException e) {
      LOG.info("Could not initialize Clusterer, is the plugin enabled?");
      return;
    }
  }
}

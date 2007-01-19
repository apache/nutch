<%--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
--%>
<%

// @author Dawid Weiss
//
// PERFORMANCE/USER INTERFACE NOTE:
//
// What I do here is merely a demonstration. In real life the clustering
// process should be done in a separate "processing" stream, most likely
// a separate HTML frame that the user's browser requests data to.
// We don't want the user to wait with plain snippets until the clusters
// are created.
//
// Also: clustering is resource consuming, so a cache of recent queries 
// would be in place. Besides, such cache would also be beneficial for the
// purpose of re-querying existing clusters (remember that the
// clustering extension may be a heuristic returning a DIFFERENT set of
// clusters for an identical input).
// See www.vivisimo.com for details of how this can be done using frames, or
// http://carrot.cs.put.poznan.pl for an example of a Javascript solution.

// cluster the hits
HitsCluster [] clusters = null;
if (clusterer != null) {
  final long clusteringStart = System.currentTimeMillis();
  try {
    clusters = clusterer.clusterHits( details, Summary.toStrings(summaries) );
    final long clusteringDuration = System.currentTimeMillis() - clusteringStart;
    bean.LOG.info("Clustering took: " + clusteringDuration + " milliseconds.");
  } catch (Exception e) {
    // failed to do clustering (see below)
  }
}

if (clusterer == null) {
  %>No clustering extension found.<%
} else {
  if (clusters == null) {
    %>Unable to do clustering.<%
  } else if (clusters.length == 0) {
    %>No clusters found.<%
  } else {
    // display top N clusters and top Q documents inside them.
    int N = 10;
    int Q = 3;
    int maxLabels = 2;
    
    int displayCounter = 0;
    N = Math.min(N, clusters.length );

    for (int clusterIndex = 0 ; clusterIndex < N ; clusterIndex++) {
      HitsCluster cluster = clusters[ clusterIndex ];
      String [] clusterLabels = cluster.getDescriptionLabels();
      
      // probably leave it on for now
      //if (cluster.isJunkCluster()) continue;

      // output cluster label.
      %><div style="margin: 0px; padding: 0px; font-weight: bold;"><%
      for (int k=0;k<maxLabels && k<clusterLabels.length;k++) {
        if (k>0) out.print(", ");
        out.print( Entities.encode(clusterLabels[k]) );
      }
      %></div><%
       
      // now output sample documents from the inside
      HitDetails[] documents = cluster.getHits();
      if (documents.length > 0) {
        %><ul style="font-size: 90%; margin-top: .5em;"><%
        for (int k = 0; k < Q && k < documents.length; k++) {
          HitDetails detail = documents[ k ];
          String title = detail.getValue("title");
          String url = detail.getValue("url");
          if (title == null || title.equals("")) title = url;
          if (title.length() > 35) title = title.substring(0,35) + "...";
          %>
            <li><a href="<%=url%>"><%= Entities.encode(title) %></a></li>
          <%
        }
        %></ul><%
      }
       
      // ignore subclusters for now, ALTHOUGH HIERARCHICAL CLUSTERING
      // METHODS DO EXIST AND ARE VERY USEFUL
      // HitsCluster [] subclusters = cluster.getSubclusters();
    }
  }
}

%>

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.File;

import org.apache.nutch.clustering.HitsCluster;
import org.apache.nutch.searcher.Hit;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.Hits;
import org.apache.nutch.searcher.NutchBean;
import org.apache.nutch.searcher.Query;
import junit.framework.TestCase;

/**
 * A test case for the Carrot2-based clusterer plugin to Nutch.
 *
 * <p><b>This test case is mostly commented-out because I don't know
 * how to integrate a test that requires an existing Nutch index.</b></p>
 *
 * @author Dawid Weiss
 * @version $Id: ClustererTest.java,v 1.1 2004/08/09 23:23:53 johnnx Exp $
 */
public class ClustererTest extends TestCase {

  public ClustererTest(String s) {
    super(s);
  }
  
  public ClustererTest() {
    super();
  }

  public void testEmptyInput() {
    Clusterer c = new Clusterer();
    
    HitDetails [] hitDetails = new HitDetails[0];
    String [] descriptions = new String [0];

    HitsCluster [] clusters = c.clusterHits(hitDetails, descriptions);
    assertTrue( clusters != null && clusters.length == 0 );
  }

  /*
  
  UNCOMMENT THIS IF YOU HAVE A NUTCH INDEX AVAILABLE. REPLACE
  THE HARDCODED PATH TO IT.
  
  public void testSomeInput() throws Exception {
    Clusterer c = new Clusterer();

    NutchBean bean = new NutchBean(
      new File("c:\\dweiss\\data\\mozdex-nutch\\nutch-mozdex\\resin"));
    Query q = Query.parse( "blog" );
    Hits hits = bean.search(q, 100);

    Hit[] show = hits.getHits(0, 100);
    HitDetails[] details = bean.getDetails(show);
    String[] summaries = bean.getSummary(details, q);

    HitsCluster [] clusters = c.clusterHits(details, summaries);
    assertTrue( clusters != null );
    
    for (int i=0;i<clusters.length;i++) {
        HitsCluster cluster = clusters[i];
        dump(0, cluster);
    }
  }    
  */
  
  private void dump(int level, HitsCluster cluster) {
    String [] labels = cluster.getDescriptionLabels();
    for (int indent = 0; indent<level; indent++) {
      System.out.print( "   " );
    }
    System.out.print(">> ");
    if (cluster.isJunkCluster()) System.out.print("(Junk) ");
    System.out.print("CLUSTER: ");
    for (int i=0;i<labels.length;i++) {
      System.out.print( labels[i] + "; " );
    }
    System.out.println();
    
    HitsCluster [] subclusters = cluster.getSubclusters();
    if (subclusters != null) {
      for (int i=0;i<subclusters.length;i++) {
        dump(level + 1, subclusters[i]);
      }
    }
    
    // dump documents.
    HitDetails [] hits = cluster.getHits();
    if (hits != null) {
      for (int i=0;i<hits.length;i++ ) {
        for (int indent = 0; indent<level; indent++) {
          System.out.print( "   " );
        }
        System.out.print( hits[i].getValue("url") );
        System.out.print( "; " );
        System.out.println( hits[i].getValue("title") );
      }
    }
  }
}

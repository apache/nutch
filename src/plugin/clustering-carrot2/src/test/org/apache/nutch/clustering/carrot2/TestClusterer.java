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

import java.io.InputStream;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.clustering.HitsCluster;
import org.apache.nutch.searcher.HitDetails;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * A test case for the Carrot2-based clusterer plugin to Nutch.
 */
public class TestClusterer extends TestCase {
  private Clusterer c;
  
  public TestClusterer(String testName) {
    super(testName);
  }
  
  protected void setUp() throws Exception {
    c = new Clusterer();
    c.setConf(new Configuration());
  }
  
  /**
   * The clusterer should not fail on empty input, returning
   * an empty array of {@link HitsCluster}.
   */
  public void testEmptyInput() {
    final HitDetails [] hitDetails = new HitDetails[0];
    final String [] descriptions = new String [0];
    final HitsCluster [] clusters = c.clusterHits(hitDetails, descriptions);
    assertTrue(clusters != null && clusters.length == 0);
  }

  /**
   * Tests the clusterer on some cached data.
   */
  public void testOnCachedData() throws Exception {
    final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    final DocumentBuilder parser = factory.newDocumentBuilder();
    final InputStream is = getClass().getResourceAsStream("test-input.xml");
    assertNotNull("test-input.xml not found", is);
    final Document document = parser.parse(is);
    is.close();

    final Element data = document.getDocumentElement();
    final NodeList docs = data.getElementsByTagName("document");
    
    final ArrayList summaries = new ArrayList();
    final ArrayList hitDetails = new ArrayList();

    assertTrue(docs.getLength() > 0);
    for (int i = 0; i < docs.getLength(); i++) {
      final Element doc = (Element) docs.item(i);
      assertTrue(doc.getNodeType() == Node.ELEMENT_NODE);
      final Element urlElement = (Element) doc.getElementsByTagName("url").item(0);
      final Element snippetElement = (Element) doc.getElementsByTagName("snippet").item(0);
      final Element titleElement = (Element) doc.getElementsByTagName("title").item(0);

      summaries.add(toText(titleElement) + " " + toText(snippetElement));
      hitDetails.add(new HitDetails(
          new String [] {"url"}, 
          new String [] {toText(urlElement)}));
    }

    HitsCluster [] clusters = c.clusterHits(
        (HitDetails[]) hitDetails.toArray(new HitDetails[hitDetails.size()]),
        (String[]) summaries.toArray(new String[summaries.size()]));
    
    // There should be SOME clusters in the input... words distribution
    // should not be random because some words have higher probability.
    assertTrue(clusters != null);
    assertTrue("Clusters expected, but not found.", clusters.length > 0);

    // Check hit references inside clusters.
    for (int i = 0; i < clusters.length; i++) {
      assertTrue(clusters[i].getHits().length > 0);
    }

    /*
    // Dump cluster content if you need to.
    System.out.println("Clusters: " + clusters.length);
    for (int i = 0; i < clusters.length; i++) {
      dump(0, clusters[i]);
    }
    */
  }
  
  /**
   * Converts a {@link Element} to plain text.
   */
  private String toText(Element snippetElement) {
    final StringBuffer buffer = new StringBuffer();
    final NodeList list = snippetElement.getChildNodes();
    for (int i = 0; i < list.getLength(); i++) {
      Node n = list.item(i);
      if (n.getNodeType() == Node.TEXT_NODE) {
        buffer.append(n.getNodeValue());
      } else if (n.getNodeType() == Node.CDATA_SECTION_NODE) {
        n.getNodeValue();
      } else throw new RuntimeException("Unexpected nested element when converting to text.");
    }
    return buffer.toString();
  }

  /**
   * Dumps the content of {@link HitsCluster} to system output stream. 
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

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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.Iterator;

import org.apache.nutch.clustering.HitsCluster;
import org.apache.nutch.clustering.OnlineClusterer;
import org.apache.nutch.searcher.HitDetails;
import com.dawidweiss.carrot.core.local.*;
import com.dawidweiss.carrot.core.local.clustering.RawCluster;
import com.dawidweiss.carrot.core.local.impl.ClustersConsumerOutputComponent;
import com.dawidweiss.carrot.util.tokenizer.SnippetTokenizerLocalFilterComponent;
import com.stachoodev.carrot.filter.lingo.local.LingoLocalFilterComponent;

import com.dawidweiss.carrot.util.tokenizer.languages.dutch.Dutch;
import com.dawidweiss.carrot.util.tokenizer.languages.english.English;
import com.dawidweiss.carrot.util.tokenizer.languages.french.French;
import com.dawidweiss.carrot.util.tokenizer.languages.german.German;
import com.dawidweiss.carrot.util.tokenizer.languages.italian.Italian;
import com.dawidweiss.carrot.util.tokenizer.languages.spanish.Spanish;
import com.dawidweiss.carrot.core.local.linguistic.Language;


/**
 * An plugin providing an implementation of {@link OnlineClusterer} extension
 * using clustering components of the Carrot2 project
 * (<a href="http://carrot2.sourceforge.net">http://carrot2.sourceforge.net</a>). 
 *
 * @author Dawid Weiss
 * @version $Id: Clusterer.java,v 1.1 2004/08/09 23:23:53 johnnx Exp $
 */
public class Clusterer implements OnlineClusterer {
  private final LocalController controller;

  /**
   * An empty public constructor for making new instances
   * of the clusterer.
   */
  public Clusterer() {
    controller = new LocalControllerBase();
    addComponentFactories();
    addProcesses();
  }

  /** Adds the required component factories to a local Carrot2 controller. */
  private void addComponentFactories() {
    // Local nutch input component
    LocalComponentFactory nutchInputFactory = new LocalComponentFactoryBase() {
      public LocalComponent getInstance() {
        return new LocalNutchInputComponent();
      }
    };
    controller.addLocalComponentFactory("input.localnutch", nutchInputFactory);
    
    // Cluster consumer output component
    LocalComponentFactory clusterConsumerOutputFactory = new LocalComponentFactoryBase() {
      public LocalComponent getInstance() {
        return new ClustersConsumerOutputComponent();
      }
    };
    controller.addLocalComponentFactory("output.cluster-consumer", 
      clusterConsumerOutputFactory);
    
    // Clustering component here.
    LocalComponentFactory lingoFactory = new LocalComponentFactoryBase() {
      public LocalComponent getInstance() {
        HashMap defaults = new HashMap();
        
        // These are adjustments settings for the clustering algorithm...
        // You can play with them, but the values below are our 'best guess'
        // settings that we acquired experimentally.
        defaults.put("lsi.threshold.clusterAssignment", "0.150");
        defaults.put("lsi.threshold.candidateCluster",  "0.775");

        // TODO: this should be eventually replaced with documents from Nutch
        // tagged with a language tag. There is no need to again determine
        // the language of a document.
        return new LingoLocalFilterComponent(
          // If you want to include Polish in the list of supported languages,
          // you have to download a separate Carrot2-component called
          // carrot2-stemmer-lametyzator.jar, put it in classpath
          // and add new Polish() below.
          new Language[]
          { 
            new English(), 
            new Dutch(), 
            new French(), 
            new German(),
            new Italian(), 
            new Spanish() 
          }, defaults);
      }
    };
    controller.addLocalComponentFactory("filter.lingo-old", lingoFactory);      
  }

  /** Adds a clustering process to the local controller */  
  private void addProcesses() {
    LocalProcessBase lingoNMFKM3 
      = new LocalProcessBase(
        "input.localnutch",
        "output.cluster-consumer",
        new String [] {"filter.lingo-old"},
        "Example the Lingo clustering algorithm.",
        "");

    try {
      controller.addProcess("lingo-nmf-km-3", lingoNMFKM3);
    } catch (Exception e) {
      throw new RuntimeException("Could not assemble clustering process.", e);
    }
  }
  
  /**
   * See {@link OnlineClusterer} for documentation.
   */
  public HitsCluster [] clusterHits(HitDetails [] hitDetails, String [] descriptions) {
    Map requestParams = new HashMap();
    requestParams.put(LocalNutchInputComponent.NUTCH_INPUT_HIT_DETAILS_ARRAY,
      hitDetails);
    requestParams.put(LocalNutchInputComponent.NUTCH_INPUT_SUMMARIES_ARRAY,
      descriptions);
    try {
      ProcessingResult result = 
        controller.query("lingo-nmf-km-3", "pseudo-query", requestParams);

      ClustersConsumerOutputComponent.Result output =
        (ClustersConsumerOutputComponent.Result) result.getQueryResult();

      List outputClusters = output.clusters;
      HitsCluster [] clusters = new HitsCluster[ outputClusters.size() ];

      int j = 0;
      for (Iterator i = outputClusters.iterator(); i.hasNext(); j++) {
        RawCluster rcluster = (RawCluster) i.next();
        clusters[j] = new HitsClusterAdapter(rcluster, hitDetails);
      }

      // invoke Carrot2 process here.
      return clusters;
    } catch (MissingProcessException e) {
      throw new RuntimeException("Missing clustering process.", e);
    } catch (Exception e) {
      throw new RuntimeException("Unidentified problems with the clustering.", e);
    }
  }
}

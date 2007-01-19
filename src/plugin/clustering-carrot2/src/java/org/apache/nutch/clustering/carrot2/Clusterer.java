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

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.clustering.HitsCluster;
import org.apache.nutch.clustering.OnlineClusterer;
import org.apache.nutch.searcher.HitDetails;

import com.dawidweiss.carrot.core.local.*;
import com.dawidweiss.carrot.core.local.clustering.RawCluster;
import com.dawidweiss.carrot.core.local.impl.ClustersConsumerOutputComponent;
import com.dawidweiss.carrot.core.local.linguistic.Language;
import com.dawidweiss.carrot.util.tokenizer.languages.AllKnownLanguages;
import com.stachoodev.carrot.filter.lingo.local.LingoLocalFilterComponent;


/**
 * An plugin providing an implementation of {@link OnlineClusterer} 
 * extension using clustering components of the Carrot2 project
 * (<a href="http://carrot2.sourceforge.net">http://carrot2.sourceforge.net</a>).
 * 
 * We hardcode the following Carrot2 process:
 * <pre><![CDATA[
 * <local-process id="yahoo-lingo">
 *   <name>Yahoo Search API -- Lingo Classic Clusterer</name>
 * 
 *   <input  component-key="input-localnutch" />
 *   <filter component-key="filter-lingo" />
 *   <output component-key="output-clustersConsumer" />
 * </local-process>
 * ]]></pre>
 *
 * @author Dawid Weiss
 * @version $Id: Clusterer.java,v 1.1 2004/08/09 23:23:53 johnnx Exp $
 */
public class Clusterer implements OnlineClusterer, Configurable {
  /** Default language property name. */
  private final static String CONF_PROP_DEFAULT_LANGUAGE =
    "extension.clustering.carrot2.defaultLanguage";

  /** Recognizable languages property name. */
  private final static String CONF_PROP_LANGUAGES =
    "extension.clustering.carrot2.languages";

  /** Internal clustering process ID in Carrot2 LocalController */
  private final static String PROCESS_ID = "nutch-lingo";
  
  public static final Log logger = LogFactory.getLog(Clusterer.class);  

  /** The LocalController instance used for clustering */
  private LocalController controller;

  /** Nutch configuration. */
  private Configuration conf;

  /** 
   * Default language for hits. English by default, but may be changed
   * via a property in Nutch configuration. 
   */
  private String defaultLanguage = "en";

  /** 
   * A list of recognizable languages..
   * English only by default, but configurable via Nutch configuration.
   */
  private String [] languages = new String [] {defaultLanguage};

  /**
   * An empty public constructor for making new instances
   * of the clusterer.
   */
  public Clusterer() {
    initialize();
  }

  private synchronized void initialize() {
    controller = new LocalControllerBase();
    addComponentFactories();
    addProcesses();
  }

  /** Adds the required component factories to a local Carrot2 controller. */
  private void addComponentFactories() {
    //  *   <input  component-key="input-localnutch" />
    LocalComponentFactory nutchInputFactory = new LocalComponentFactoryBase() {
      public LocalComponent getInstance() {
        return new LocalNutchInputComponent(defaultLanguage);
      }
    };
    controller.addLocalComponentFactory("input-localnutch", nutchInputFactory);

    // *   <filter component-key="filter-lingo" />
    LocalComponentFactory lingoFactory = new LocalComponentFactoryBase() {
      public LocalComponent getInstance() {
        HashMap defaults = new HashMap();

        // These are adjustments settings for the clustering algorithm.
        // If you try the live WebStart demo of Carrot2 you can see how they affect
        // the final clustering: http://www.carrot2.org/webstart 
        defaults.put("lsi.threshold.clusterAssignment", "0.150");
        defaults.put("lsi.threshold.candidateCluster",  "0.775");

        // Initialize a new Lingo clustering component.
        ArrayList languageList = new ArrayList(languages.length);
        for (int i = 0; i < languages.length; i++) {
          final String lcode = languages[i];
          try {
            Language lang = AllKnownLanguages.getLanguageForIsoCode(lcode);
            if (lang == null) {
              if (logger.isWarnEnabled()) {
                logger.warn("Language not supported in Carrot2: " + lcode);
              }
            } else {
              languageList.add(lang);
              if (logger.isDebugEnabled()) {
                logger.debug("Language loaded: " + lcode);
              }
            }
          } catch (Throwable t) {
            if (logger.isWarnEnabled()) {
              logger.warn("Language could not be loaded: " + lcode, t);
            }
          }
        }
        return new LingoLocalFilterComponent(
          (Language []) languageList.toArray(new Language [languageList.size()]), defaults);
      }
    };
    controller.addLocalComponentFactory("filter-lingo", lingoFactory);

    // *   <output component-key="output-clustersConsumer" />
    LocalComponentFactory clusterConsumerOutputFactory = new LocalComponentFactoryBase() {
      public LocalComponent getInstance() {
        return new ClustersConsumerOutputComponent();
      }
    };
    controller.addLocalComponentFactory("output-clustersConsumer", 
      clusterConsumerOutputFactory);
  }

  /** 
   * Adds a hardcoded clustering process to the local controller.
   */  
  private void addProcesses() {
    LocalProcessBase process = new LocalProcessBase(
        "input-localnutch",                                   // input
        "output-clustersConsumer",                            // output
        new String [] {"filter-lingo"},                       // filters
        "The Lingo clustering algorithm (www.carrot2.org).",
        "");

    try {
      controller.addProcess(PROCESS_ID, process);
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
      // The input component takes Nutch's results so we don't need the query argument.
      final ProcessingResult result = 
        controller.query(PROCESS_ID, "no-query", requestParams);

      final ClustersConsumerOutputComponent.Result output =
        (ClustersConsumerOutputComponent.Result) result.getQueryResult();

      final List outputClusters = output.clusters;
      final HitsCluster [] clusters = new HitsCluster[ outputClusters.size() ];

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

  /**
   * Implementation of {@link Configurable}
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    
    // Configure default language and other component settings.
    if (conf.get(CONF_PROP_DEFAULT_LANGUAGE) != null) {
      // Change the default language.
      this.defaultLanguage = conf.get(CONF_PROP_DEFAULT_LANGUAGE);
    } 
    if (conf.getStrings(CONF_PROP_LANGUAGES) != null) {
      this.languages = conf.getStrings(CONF_PROP_LANGUAGES);
    }

    if (logger.isInfoEnabled()) {
      logger.info("Default language: " + defaultLanguage);
      logger.info("Enabled languages: " + Arrays.asList(languages));
    }

    initialize();
  }

  /**
   * Implementation of {@link Configurable}
   */
  public Configuration getConf() {
    return conf;
  }
}

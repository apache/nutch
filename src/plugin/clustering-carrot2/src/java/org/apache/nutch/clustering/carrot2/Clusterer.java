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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.nutch.clustering.HitsCluster;
import org.apache.nutch.clustering.OnlineClusterer;
import org.apache.nutch.searcher.HitDetails;
import org.carrot2.core.DuplicatedKeyException;
import org.carrot2.core.InitializationException;
import org.carrot2.core.LocalComponent;
import org.carrot2.core.LocalComponentFactory;
import org.carrot2.core.LocalControllerBase;
import org.carrot2.core.LocalProcess;
import org.carrot2.core.LocalProcessBase;
import org.carrot2.core.MissingComponentException;
import org.carrot2.core.MissingProcessException;
import org.carrot2.core.ProcessingResult;
import org.carrot2.core.clustering.RawCluster;
import org.carrot2.core.controller.ControllerHelper;
import org.carrot2.core.controller.LoaderExtensionUnknownException;
import org.carrot2.core.impl.ArrayOutputComponent;
import org.carrot2.core.linguistic.Language;
import org.carrot2.filter.lingo.local.LingoLocalFilterComponent;
import org.carrot2.util.tokenizer.languages.AllKnownLanguages;



/**
 * This plugin provides an implementation of {@link OnlineClusterer} 
 * extension using clustering components of the Carrot2 project
 * (<a href="http://www.carrot2.org">http://www.carrot2.org</a>).
 * 
 * <p>This class hardcodes an equivalent of the following Carrot2 process:
 * <pre><![CDATA[
 * <local-process id="yahoo-lingo">
 *   <name>Yahoo Search API -- Lingo Classic Clusterer</name>
 * 
 *   <input  component-key="input-nutch" />
 *   <filter component-key="filter-lingo" />
 *   <output component-key="output-clustersConsumer" />
 * </local-process>
 * ]]></pre>
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
  private LocalControllerBase controller;

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
    // Don't forget to call {@link #setConf(Configuration)}.
  }

  /**
   * See {@link OnlineClusterer} for documentation.
   */
  public HitsCluster [] clusterHits(HitDetails [] hitDetails, String [] descriptions) {
    if (this.controller == null) {
      logger.error("initialize() not called.");
      return new HitsCluster[0];
    }

    final Map requestParams = new HashMap();
    requestParams.put(NutchInputComponent.NUTCH_INPUT_HIT_DETAILS_ARRAY,
      hitDetails);
    requestParams.put(NutchInputComponent.NUTCH_INPUT_SUMMARIES_ARRAY,
      descriptions);

    try {
      // The input component takes Nutch's results so we don't need the query argument.
      final ProcessingResult result = 
        controller.query(PROCESS_ID, "no-query", requestParams);

      final ArrayOutputComponent.Result output =
        (ArrayOutputComponent.Result) result.getQueryResult();

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
  
  /**
   * Initialize clustering processes and Carrot2 components.
   */
  private synchronized void initialize() {
    // Initialize language list, temporarily switching off logging
    // of warnings. This is a bit of a hack, but we don't want to
    // redistribute the entire Carrot2 distro and this prevents
    // nasty ClassNotFound warnings.
    final Logger c2Logger = Logger.getLogger("org.carrot2");
    final Level original = c2Logger.getLevel();
    c2Logger.setLevel(Level.ERROR);
    AllKnownLanguages.getLanguageCodes();
    c2Logger.setLevel(original);

    // Initialize the controller.    
    controller = new LocalControllerBase();

    final Configuration nutchConf = getConf();
    final String processResource = nutchConf.get(
        "extension.clustering.carrot2.process-resource");

    if (processResource == null) {
      logger.info("Using default clustering algorithm (Lingo).");
      addDefaultProcess();
    } else {
      logger.info("Using custom clustering process: " + processResource);
      controller.setComponentAutoload(true);
      
      final ControllerHelper helper = new ControllerHelper();
      final InputStream is = Thread.currentThread()
        .getContextClassLoader().getResourceAsStream(processResource);
      if (is != null) {
        try {
          final LocalComponentFactory nutchInputFactory = new LocalComponentFactory() {
            public LocalComponent getInstance() {
              return new NutchInputComponent(defaultLanguage);
            }
          };
          controller.addLocalComponentFactory("input-nutch", nutchInputFactory);
          
          final LocalProcess process = helper.loadProcess(
              helper.getExtension(processResource), is).getProcess();
          controller.addProcess(PROCESS_ID, process);
          is.close();
        } catch (IOException e) {
          logger.error("Could not load process resource: " + processResource, e);
        } catch (LoaderExtensionUnknownException e) {
          logger.error("Unrecognized extension of process resource: " + processResource);
        } catch (InstantiationException e) {
          logger.error("Could not instantiate process: " + processResource, e);
        } catch (InitializationException e) {
          logger.error("Could not initialize process: " + processResource, e);
        } catch (DuplicatedKeyException e) {
          logger.error("Duplicated key (unreachable?): " + processResource, e);
        } catch (MissingComponentException e) {
          logger.error("Some components are missing, could not initialize process: " 
              + processResource, e);
        }
      } else {
        logger.error("Could not find process resource: " + processResource);
      }
    }
  }

  /**
   * Adds a default clustering process using Lingo algorithm.
   */
  private void addDefaultProcess() {
    try {
      addComponentFactories();
      addProcesses();
    } catch (DuplicatedKeyException e) {
      logger.fatal("Duplicated component or process identifier.", e);
    }
  }

  /** Adds the required component factories to a local Carrot2 controller. */
  private void addComponentFactories() throws DuplicatedKeyException {
    //  *   <input  component-key="input-nutch" />
    LocalComponentFactory nutchInputFactory = new LocalComponentFactory() {
      public LocalComponent getInstance() {
        return new NutchInputComponent(defaultLanguage);
      }
    };
    controller.addLocalComponentFactory("input-nutch", nutchInputFactory);

    // *   <filter component-key="filter-lingo" />
    LocalComponentFactory lingoFactory = new LocalComponentFactory() {
      public LocalComponent getInstance() {
        final HashMap defaults = new HashMap();

        // These are adjustments settings for the clustering algorithm.
        // If you try the live WebStart demo of Carrot2 you can see how they affect
        // the final clustering: http://www.carrot2.org 
        defaults.put("lsi.threshold.clusterAssignment", "0.150");
        defaults.put("lsi.threshold.candidateCluster",  "0.775");

        // Initialize a new Lingo clustering component.
        ArrayList languageList = new ArrayList(languages.length);
        for (int i = 0; i < languages.length; i++) {
          final String lcode = languages[i];
          try {
            final Language lang = AllKnownLanguages.getLanguageForIsoCode(lcode);
            if (lang == null) {
              logger.warn("Language not supported in Carrot2: " + lcode);
            } else {
              languageList.add(lang);
              logger.debug("Language loaded: " + lcode);
            }
          } catch (Throwable t) {
              logger.warn("Language could not be loaded: " + lcode, t);
          }
        }
        return new LingoLocalFilterComponent(
          (Language []) languageList.toArray(new Language [languageList.size()]), defaults);
      }
    };
    controller.addLocalComponentFactory("filter-lingo", lingoFactory);

    // *   <output component-key="output-clustersConsumer" />
    LocalComponentFactory clusterConsumerOutputFactory = new LocalComponentFactory() {
      public LocalComponent getInstance() {
        return new ArrayOutputComponent();
      }
    };
    controller.addLocalComponentFactory("output-array", 
      clusterConsumerOutputFactory);
  }

  /** 
   * Adds a hardcoded clustering process to the local controller.
   */  
  private void addProcesses() {
    final LocalProcessBase process = new LocalProcessBase(
        "input-nutch",
        "output-array",
        new String [] {"filter-lingo"},
        "The Lingo clustering algorithm (www.carrot2.org).",
        "");

    try {
      controller.addProcess(PROCESS_ID, process);
    } catch (Exception e) {
      throw new RuntimeException("Could not assemble clustering process.", e);
    }
  }  
}

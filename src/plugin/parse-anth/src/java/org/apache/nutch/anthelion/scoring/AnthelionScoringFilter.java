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
package org.apache.nutch.anthelion.scoring;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.anthelion.models.AnthURL;
import org.apache.nutch.anthelion.parsing.WdcParser;
import org.apache.nutch.anthelion.scoring.classifier.NutchOnlineClassifier;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;
// Slf4j Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The plugin is based on the URL selection strategy approach: Anthelion. The
 * plugin uses online classifier for predicting urls that might contain semantic
 * data.
 * 
 * @author Petar Ristoski (petar@dwslab.de)
 *
 */
public class AnthelionScoringFilter implements ScoringFilter {

  private final static Logger LOG = LoggerFactory.getLogger(AnthelionScoringFilter.class);

  private Configuration conf;
  private NutchOnlineClassifier onlineClassifier;
  private Properties configProp = new Properties();

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {

    this.conf = conf;

    // get the configurations for the model
    // they are defined in a separate file
    // TODO: put the configs in the nutch-anth.xml
    String inputConfFile = getConf().get("anth.scoring.classifier.PropsFilePath");

    try {
      InputStream in = new FileInputStream(inputConfFile);
      configProp.load(in);
      System.out.println("Properties loaded.");
    } catch (IOException e) {
      System.out.println("Could not load properties.");
      e.printStackTrace();
      System.exit(0);
    }
    onlineClassifier = new NutchOnlineClassifier(configProp.getProperty("classifier.name"),
        configProp.getProperty("classifier.options"),
        Integer.parseInt(configProp.getProperty("classifier.hashtricksize")),
        Integer.parseInt(configProp.getProperty("classifier.learn.batchsize")));
  }

  /**
   * Pushes feedback in the online classifier for the current web page and
   * sets the containsSemFather field for the outlinks
   * 
   *
   */
  @Override
  public void passScoreAfterParsing(Text url, Content content, Parse parse) throws ScoringFilterException {

    LOG.info("------>WE ARE PUSHING FEEDBACK------:");
    // get the feedback (if the web page contains semantic data)
    boolean containsSem = false;
    boolean semFather = false;
    try {
      // check the current score
      // float score =
      // Float.parseFloat(parse.getData().getContentMeta().get(Nutch.SCORE_KEY));
      // LOG.info("------>THE CURRENT SCORE IS------:" + score);
      containsSem = Boolean.parseBoolean(parse.getData().getMeta(WdcParser.META_CONTAINS_SEM));
      LOG.info("------>GOT THE contains sem------:" + containsSem);
      semFather = Boolean.parseBoolean(parse.getData().getMeta(WdcParser.META_CONTAINS_SEM_FATHER));

    } catch (Exception e) {
      LOG.info("ERROR GETTING THE META DATA" + e.getMessage());
    }
    // create AnthURL

    try {
      AnthURL anthUrl = new AnthURL(hash(url.toString()), new URI(url.toString()), semFather, !semFather, false,
          false, containsSem);
      // now update the classifier
      onlineClassifier.pushFeedback(anthUrl);
    } catch (Exception uriE) {
      LOG.info("COULD NOT create proper URI from" + uriE.getMessage());
      System.out.println("Could not create proper URI from: " + url + ". Skipping.");
    }
    // tell the outlinks that their parent has semantic data
    parse.getData().getContentMeta().set(WdcParser.META_CONTAINS_SEM_FATHER_FOR_SUB, Boolean.toString(containsSem));
  }

  /**
   * Classifies the outlinks using the online classifier, and sets their score
   * which is used in the fetching process.
   * 
   *
   */
  @Override
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl, ParseData parseData,
      Collection<Entry<Text, CrawlDatum>> targets, CrawlDatum adjust, int allCount)
          throws ScoringFilterException {
    LOG.info("------>WE ARE CALCULATING THE SCORE FOR THE OUTLINKS------:");
    // get the sem of the father
    Boolean semFather = Boolean.parseBoolean(parseData.getMeta(WdcParser.META_CONTAINS_SEM_FATHER_FOR_SUB));

    for (Entry<Text, CrawlDatum> target : targets) {
      // this is the default score
      float score = 0.5f;
      // classify the new url
      // first store the semFather for further usage
      target.getValue().getMetaData().put(new Text(WdcParser.META_CONTAINS_SEM_FATHER),
          new Text(Boolean.toString(semFather)));
      // create the AnthURL
      try {
        URI tmpURI = new URI(target.getKey().toString());
        AnthURL anthUrl = new AnthURL(hash(target.getKey().toString()), tmpURI, semFather, !semFather, false,
            false, false);
        onlineClassifier.classifyUrl(anthUrl);
        // the score is really small, and it is getting lost in the
        // conversion so we have to increase it
        double prediction = anthUrl.prediction * 100000000;
        LOG.info("THE PREDICTED SCORE of {} IS {}", target.getKey(), prediction);

        score += prediction;
      } catch (Exception e) {
        // TODO Auto-generated catch block
        LOG.info("ERROR SETTING THE NEW SCORE FOR THE OUTLINK" + e.getMessage());
        e.printStackTrace();
        score = 0.5f;
      }
      // this is only for the experiments
      if (semFather)
        score += 0.5;

      target.getValue().setScore((float) score);
      LOG.info("Setting score of {} to {}", target.getKey(), score);
    }
    return adjust;
  }

  @Override
  public void injectedScore(Text url, CrawlDatum datum) throws ScoringFilterException {
    // TODO Auto-generated method stub

  }

  @Override
  public void initialScore(Text url, CrawlDatum datum) throws ScoringFilterException {
    // TODO Auto-generated method stub

  }

  /**
   * This is the score that is used for selecting the urls that are going to
   * be fetched. If you didn't know that you will have some headaches.
   * 
   */
  @Override
  public float generatorSortValue(Text url, CrawlDatum datum, float initSort) throws ScoringFilterException {
    // TODO Auto-generated method stub
    return datum.getScore();
  }

  @Override
  public void passScoreBeforeParsing(Text url, CrawlDatum datum, Content content) throws ScoringFilterException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum, List<CrawlDatum> inlinked)
      throws ScoringFilterException {
    // TODO Auto-generated method stub

  }

  @Override
  public float indexerScore(Text url, NutchDocument doc, CrawlDatum dbDatum, CrawlDatum fetchDatum, Parse parse,
      Inlinks inlinks, float initScore) throws ScoringFilterException {
    // TODO Auto-generated method stub
    return 0;
  }

  /**
   * Gives (almost) unique Id for a given string.
   * 
   * @param s
   * @return
   */
  public static long hash(String s) {
    long h = 0;
    for (int i = 0; i < s.length(); i++) {
      h = 31 * h + s.charAt(i);
    }
    return h;
  }
}
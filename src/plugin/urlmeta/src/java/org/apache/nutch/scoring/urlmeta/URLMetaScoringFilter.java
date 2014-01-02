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

package org.apache.nutch.scoring.urlmeta;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;

/**
 * For documentation:
 * 
 * @see URLMetaIndexingFilter
 */
public class URLMetaScoringFilter extends Configured implements ScoringFilter {

  private static final Logger LOG = LoggerFactory.getLogger(URLMetaScoringFilter.class);
  private static final String CONF_PROPERTY = "urlmeta.tags";
  private static String[] urlMetaTags;
  private Configuration conf;

  /**
   * This will take the metatags that you have listed in your "urlmeta.tags"
   * property, and looks for them inside the parseData object. If they exist,
   * this will be propagated into your 'targets' Collection's ["outlinks"]
   * attributes.
   * 
   * @see ScoringFilter#distributeScoreToOutlinks
   */
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl,
      ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
      CrawlDatum adjust, int allCount) throws ScoringFilterException {
    if (urlMetaTags == null || targets == null || parseData == null)
      return adjust;

    Iterator<Entry<Text, CrawlDatum>> targetIterator = targets.iterator();

    while (targetIterator.hasNext()) {
      Entry<Text, CrawlDatum> nextTarget = targetIterator.next();

      for (String metatag : urlMetaTags) {
        String metaFromParse = parseData.getMeta(metatag);

        if (metaFromParse == null)
          continue;

        nextTarget.getValue().getMetaData().put(new Text(metatag),
            new Text(metaFromParse));
      }
    }
    return adjust;
  }

  /**
   * Takes the metadata, specified in your "urlmeta.tags" property, from the
   * datum object and injects it into the content. This is transfered to the
   * parseData object.
   * 
   * @see ScoringFilter#passScoreBeforeParsing
   * @see URLMetaScoringFilter#passScoreAfterParsing
   */
  public void passScoreBeforeParsing(Text url, CrawlDatum datum, Content content) {
    if (urlMetaTags == null || content == null || datum == null)
      return;

    for (String metatag : urlMetaTags) {
      Text metaFromDatum = (Text) datum.getMetaData().get(new Text(metatag));

      if (metaFromDatum == null)
        continue;

      content.getMetadata().set(metatag, metaFromDatum.toString());
    }
  }

  /**
   * Takes the metadata, which was lumped inside the content, and replicates it
   * within your parse data.
   * 
   * @see URLMetaScoringFilter#passScoreBeforeParsing
   * @see ScoringFilter#passScoreAfterParsing
   */
  public void passScoreAfterParsing(Text url, Content content, Parse parse) {
    if (urlMetaTags == null || content == null || parse == null)
      return;

    for (String metatag : urlMetaTags) {
      String metaFromContent = content.getMetadata().get(metatag);

      if (metaFromContent == null)
        continue;

      parse.getData().getParseMeta().set(metatag, metaFromContent);
    }
  }

  /** Boilerplate */
  public float generatorSortValue(Text url, CrawlDatum datum, float initSort)
      throws ScoringFilterException {
    return initSort;
  }

  /** Boilerplate */
  public float indexerScore(Text url, NutchDocument doc, CrawlDatum dbDatum,
      CrawlDatum fetchDatum, Parse parse, Inlinks inlinks, float initScore)
      throws ScoringFilterException {
    return initScore;
  }

  /** Boilerplate */
  public void initialScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
    return;
  }

  /** Boilerplate */
  public void injectedScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
    return;
  }

  /** Boilerplate */
  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum,
      List<CrawlDatum> inlinked) throws ScoringFilterException {
    return;
  }

  /**
   * handles conf assignment and pulls the value assignment from the
   * "urlmeta.tags" property
   */
  public void setConf(Configuration conf) {
    super.setConf(conf);

    if (conf == null)
      return;

    urlMetaTags = conf.getStrings(CONF_PROPERTY);
  }

  /** Boilerplate */
  public Configuration getConf() {
    return conf;
  }
}

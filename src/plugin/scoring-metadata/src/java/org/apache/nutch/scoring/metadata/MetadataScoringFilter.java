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
package org.apache.nutch.scoring.metadata;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.List;

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
import org.apache.nutch.scoring.AbstractScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;


/**
 * For documentation:
 * 
 * {@link org.apache.nutch.scoring.metadata}
 */
public class MetadataScoringFilter extends AbstractScoringFilter  {

  public static final String METADATA_DATUM   = "scoring.db.md";
  public static final String METADATA_CONTENT = "scoring.content.md";
  public static final String METADATA_PARSED  = "scoring.parse.md";
  private static String[] datumMetadata;
  private static String[] contentMetadata;
  private static String[] parseMetadata;
  private Configuration conf;

  /**
   * This will take the metadata that you have listed in your "scoring.parse.md"
   * property, and looks for them inside the parseData object. If they exist,
   * this will be propagated into your 'targets' Collection's ["outlinks"]
   * attributes.
   * 
   * @see ScoringFilter#distributeScoreToOutlinks
   */
  @Override
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl,
      ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
      CrawlDatum adjust, int allCount) throws ScoringFilterException {
    if (parseMetadata == null || targets == null || parseData == null)
      return adjust;

    Iterator<Entry<Text, CrawlDatum>> targetIterator = targets.iterator();

    while (targetIterator.hasNext()) {
      Entry<Text, CrawlDatum> nextTarget = targetIterator.next();

      for (String meta : parseMetadata) {
        String metaFromParse = parseData.getMeta(meta);

        if (metaFromParse == null)
          continue;

        nextTarget.getValue().getMetaData()
            .put(new Text(meta), new Text(metaFromParse));
      }
    }
    return adjust;
  }

  /**
   * Takes the metadata, specified in your "scoring.db.md" property, from the
   * datum object and injects it into the content. This is transfered to the
   * parseData object.
   * 
   * @see ScoringFilter#passScoreBeforeParsing
   * @see MetadataScoringFilter#passScoreAfterParsing
   */
  @Override
  public void passScoreBeforeParsing(Text url, CrawlDatum datum, Content content) {
    if (datumMetadata == null || content == null || datum == null)
      return;

    for (String meta : datumMetadata) {
      Text metaFromDatum = (Text) datum.getMetaData().get(new Text(meta));

      if (metaFromDatum == null) {
        continue;
      }

      content.getMetadata().set(meta, metaFromDatum.toString());
    }
  }

  /**
   * Takes the metadata, which was lumped inside the content, and replicates it
   * within your parse data.
   * 
   * @see MetadataScoringFilter#passScoreBeforeParsing
   * @see ScoringFilter#passScoreAfterParsing
   */
  @Override
  public void passScoreAfterParsing(Text url, Content content, Parse parse) {
    if (contentMetadata == null || content == null || parse == null)
      return;

    for (String meta : contentMetadata) {
      String metaFromContent = content.getMetadata().get(meta);

      if (metaFromContent == null)
        continue;

      parse.getData().getParseMeta().set(meta, metaFromContent);
    }
  }

  /**
   * handles conf assignment and pulls the value assignment from the
   * "scoring.db.md", "scoring.content.md" and "scoring.parse.md" properties.
   */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);

    if (conf == null)
      return;

    datumMetadata = conf.getStrings(METADATA_DATUM);
    contentMetadata = conf.getStrings(METADATA_CONTENT);
    parseMetadata = conf.getStrings(METADATA_PARSED);
  }
}

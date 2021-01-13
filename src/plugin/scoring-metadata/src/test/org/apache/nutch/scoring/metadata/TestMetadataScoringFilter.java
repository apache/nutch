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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.*;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class TestMetadataScoringFilter {


  @Test
  public void distributeScoreToOutlinks() throws ScoringFilterException {
    Configuration conf = NutchConfiguration.create();
    conf.set(MetadataScoringFilter.METADATA_PARSED,"parent,depth");

    MetadataScoringFilter metadataScoringFilter = new MetadataScoringFilter();
    metadataScoringFilter.setConf(conf);
    CrawlDatum crawlDatum = new CrawlDatum();

    Text from = new Text("https://nutch.apache.org/");
    ParseData parseData = new ParseData();
    String PARENT = "parent";
    String DEPTH = "depth";

    String parentMD = "https://nutch.apache.org/";
    String depthMD  = "1";
    parseData.getParseMeta().add("parent",parentMD);
    parseData.getParseMeta().add("depth",depthMD);

    HashMap<Text,CrawlDatum> targets = new HashMap();
    targets.put(new Text("https://nutch.apache.org/downloads.html"),new CrawlDatum());
    targets.put(new Text("https://wiki.apache.org/nutch"),new CrawlDatum());

    metadataScoringFilter.distributeScoreToOutlinks(from,parseData,targets.entrySet(),crawlDatum,2);

    for (CrawlDatum outlink : targets.values()){
      Text parent = (Text) outlink.getMetaData().get(new Text(PARENT));
      Text depth = (Text) outlink.getMetaData().get(new Text(DEPTH));

      Assert.assertEquals(parentMD,parent.toString());
      Assert.assertEquals(depthMD,depth.toString());
    }
  }

  @Test
  public void passScoreBeforeParsing() {
    Configuration conf = NutchConfiguration.create();
    conf.set(MetadataScoringFilter.METADATA_DATUM,"parent,depth");

    MetadataScoringFilter metadataScoringFilter = new MetadataScoringFilter();
    metadataScoringFilter.setConf(conf);
    CrawlDatum crawlDatum = new CrawlDatum();

    Text from = new Text("https://nutch.apache.org/");

    String PARENT = "parent";
    String DEPTH = "depth";

    String parentMD = "https://nutch.apache.org/";
    String depthMD  = "1";
    crawlDatum.getMetaData().put(new Text(PARENT), new Text(parentMD));
    crawlDatum.getMetaData().put(new Text(DEPTH), new Text(depthMD));
    Content content = new Content();

    metadataScoringFilter.passScoreBeforeParsing(from,crawlDatum,content);

    Assert.assertEquals(parentMD,content.getMetadata().get(PARENT));
    Assert.assertEquals(depthMD,content.getMetadata().get(DEPTH));
  }

  @Test
  public void passScoreAfterParsing() {
    Configuration conf = NutchConfiguration.create();
    conf.set(MetadataScoringFilter.METADATA_DATUM,"parent,depth");
    conf.set(MetadataScoringFilter.METADATA_CONTENT,"parent,depth");

    MetadataScoringFilter metadataScoringFilter = new MetadataScoringFilter();
    metadataScoringFilter.setConf(conf);
    CrawlDatum crawlDatum = new CrawlDatum();

    Text from = new Text("https://nutch.apache.org/");

    String PARENT = "parent";
    String DEPTH = "depth";

    String parentMD = "https://nutch.apache.org/";
    String depthMD  = "1";
    crawlDatum.getMetaData().put(new Text(PARENT), new Text(parentMD));
    crawlDatum.getMetaData().put(new Text(DEPTH), new Text(depthMD));
    Content content = new Content();
    metadataScoringFilter.passScoreBeforeParsing(from,crawlDatum,content);

    ParseData parseData = new  ParseData(ParseStatus.STATUS_SUCCESS, null, null, content.getMetadata());
    Parse parse = new ParseImpl(from.toString(),parseData);
    metadataScoringFilter.passScoreAfterParsing(from,content,parse);


    Assert.assertEquals(parentMD,parse.getData().getMeta(PARENT));
    Assert.assertEquals(depthMD,parse.getData().getMeta(DEPTH));
  }
}

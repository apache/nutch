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
package org.apache.nutch.any23;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestAny23IndexingFilter {
  @Test
  public void testAny23TriplesFields() throws Exception {
    Configuration conf = NutchConfiguration.create();
    Any23IndexingFilter filter = new Any23IndexingFilter();
    filter.setConf(conf);
    Assert.assertNotNull(filter);
    NutchDocument doc = new NutchDocument();
    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, "The Foo Page",
        new Outlink[] { }, new Metadata());
    ParseImpl parse = new ParseImpl("test page", parseData);
    String[] triples = new String[]{
        "<http://dbpedia.org/resource/Z\u00FCrich> <http://www.w3.org/2002/07/owl#sameAs> <http://rdf.freebase.com/ns/m.08966> .",
        "<http://dbpedia.org/resource/Z\u00FCrich> <http://dbpedia.org/property/yearHumidity> \"77\" .",
        "<http://dbpedia.org/resource/Z\u00FCrich> <http://www.w3.org/2000/01/rdf-schema#label> \"Zurique\"@pt ."
    };
    for (String triple : triples) {      
      parse.getData().getParseMeta().add(Any23ParseFilter.ANY23_TRIPLES, triple);
    }    
    try {
      doc = filter.filter(doc, parse, new Text("http://nutch.apache.org/"), new CrawlDatum(), new Inlinks());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
    List<Object> docTriples = doc.getField(Any23IndexingFilter.STRUCTURED_DATA).getValues();
    Assert.assertEquals(docTriples.size(), triples.length);

    Object triple = docTriples.get(0);
    Assert.assertTrue(triple instanceof Map<?, ?>);
    @SuppressWarnings("unchecked")
    Map<String, String> structuredData = (Map<String, String>) triple;
    Assert.assertEquals(structuredData.get("node"), "<http://dbpedia.org/resource/Z\u00FCrich>");
    Assert.assertEquals(structuredData.get("key"), "<http://www.w3.org/2002/07/owl#sameAs>");
    Assert.assertEquals(structuredData.get("short_key"), "sameAs");
    Assert.assertEquals(structuredData.get("value"), "<http://rdf.freebase.com/ns/m.08966>");
    
    triple = docTriples.get(1);
    Assert.assertTrue(triple instanceof Map<?, ?>);
    structuredData = (Map<String, String>) triple;
    Assert.assertEquals(structuredData.get("node"), "<http://dbpedia.org/resource/Z\u00FCrich>");
    Assert.assertEquals(structuredData.get("key"), "<http://dbpedia.org/property/yearHumidity>");
    Assert.assertEquals(structuredData.get("short_key"), "yearHumidity");
    Assert.assertEquals(structuredData.get("value"), "\"77\"");
  }
}

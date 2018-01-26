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
package org.apache.nutch.any23;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.ParserNotFound;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestAny23ParseFilter {


  private Configuration conf;

  private String fileSeparator = System.getProperty("file.separator");

  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data", ".");

  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/any23/build.xml during plugin compilation.
  private String file1 = "BBC_News_Scotland.html";
  
  private String file2 = "microdata_basic.html";

  private static final int EXPECTED_TRIPLES_1 = 79;
  
  private static final int EXPECTED_TRIPLES_2 = 39;
  
  @Before
  public void setUp() {
    this.conf = NutchConfiguration.create();
    conf.set("file.content.limit", "-1");
    conf.set("parser.timeout", "-1");
    conf.set(Any23ParseFilter.ANY_23_EXTRACTORS_CONF, "html-embedded-jsonld,html-head-icbm,html-head-links,html-head-meta,html-head-title,html-mf-adr,html-mf-geo,html-mf-hcalendar,html-mf-hcard,html-mf-hlisting,html-mf-hrecipe,html-mf-hresume,html-mf-hreview,html-mf-hreview-aggregate,html-mf-license,html-mf-species,html-mf-xfn,html-microdata,html-rdfa11,html-xpath");
    conf.set(Any23ParseFilter.ANY_23_CONTENT_TYPES_CONF, "text/html");
  }

  @Test
  public void testExtractTriplesFromHTML() throws IOException, ParserNotFound, ParseException {
    String[] triplesArray = getTriples(file1);
    
    Assert.assertEquals("We expect 117 tab-separated triples extracted by the filter", 
        EXPECTED_TRIPLES_1, triplesArray.length);
  }

  @Test
  public void extractMicroDataFromHTML() throws ParserNotFound, IOException, ParseException {
    String[] triplesArray = getTriples(file2);
    
    Assert.assertEquals("We expect 40 tab-separated triples extracted by the filter", 
        EXPECTED_TRIPLES_2, triplesArray.length);
  }

  @Test
  public void ignoreUnsupported() throws ParserNotFound, IOException, ParseException {
    String[] triplesArray = getTriples(file1, "application/pdf");

    Assert.assertEquals("We expect no triples extracted by the filter since content-type should be ignored",
            0, triplesArray.length);
  }
  
  public String[] extract(String urlString, File file, String contentType) {
    try {
      System.out.println(urlString);
      Protocol protocol = new ProtocolFactory(conf).getProtocol(urlString);
      Content content = protocol.getProtocolOutput(new Text(urlString),
          new CrawlDatum()).getContent();
      content.setContentType(contentType);
      Parse parse = new ParseUtil(conf).parse(content).get(content.getUrl());
      return parse.getData().getParseMeta().getValues(Any23ParseFilter.ANY23_TRIPLES);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.toString());
    }
    return null;
  }

  private String[] getTriples(String fileName) {
    return getTriples(fileName, "text/html");
  }

  private String[] getTriples(String fileName, String contentType) {
    String urlString = "file:" + sampleDir + fileSeparator + fileName;

    File file = new File(sampleDir + fileSeparator + fileName);

    return extract(urlString, file, contentType);
  }
}

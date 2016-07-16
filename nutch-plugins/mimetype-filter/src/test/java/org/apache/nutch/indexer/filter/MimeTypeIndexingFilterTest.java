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

package org.apache.nutch.indexer.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.util.NutchConfiguration;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * JUnit based tests of class
 * {@link org.apache.nutch.indexer.filter.MimeTypeIndexingFilter}
 *
 */
public class MimeTypeIndexingFilterTest {

  private Configuration conf = NutchConfiguration.create();
  private MimeTypeIndexingFilter filter = new MimeTypeIndexingFilter();
  private String[] MIME_TYPES = { "text/html", "image/png", "application/pdf" };
  private ParseImpl[] parses = new ParseImpl[MIME_TYPES.length];
  private String sampleDir = System.getProperty("test.data", ".");

  @Before
  public void setUp() throws Exception {
    for (int i = 0; i < MIME_TYPES.length; i++) {
      Metadata metadata = new Metadata();
      metadata.add(Response.CONTENT_TYPE, MIME_TYPES[i]);

      ParseImpl parse = new ParseImpl("text",
          new ParseData(new ParseStatus(), "title", new Outlink[0], metadata));

      parses[i] = parse;
    }
  }

  @Test
  public void testMissingConfigFile() throws Exception {
    String file = conf.get(MimeTypeIndexingFilter.MIMEFILTER_REGEX_FILE, "");
    Assert.assertEquals(String
        .format("Property %s must not be present in the the configuration file",
            MimeTypeIndexingFilter.MIMEFILTER_REGEX_FILE), "", file);

    filter.setConf(conf);

    // property not set so in this cases all documents must pass the filter
    for (int i = 0; i < parses.length; i++) {
      NutchDocument doc = filter.filter(new NutchDocument(), parses[i],
          new Text("http://www.example.com/"), new CrawlDatum(), new Inlinks());

      Assert.assertNotNull("All documents must be allowed by default", doc);
    }
  }

  @Test
  public void testAllowOnlyImages() throws Exception {
    conf.set(MimeTypeIndexingFilter.MIMEFILTER_REGEX_FILE, "allow-images.txt");
    filter.setConf(conf);

    for (int i = 0; i < parses.length; i++) {
      NutchDocument doc = filter.filter(new NutchDocument(), parses[i],
          new Text("http://www.example.com/"), new CrawlDatum(), new Inlinks());

      if (MIME_TYPES[i].contains("image")) {
        Assert.assertNotNull("Allow only images", doc);
      } else {
        Assert.assertNull("Block everything else", doc);
      }
    }
  }

  @Test
  public void testBlockHTML() throws Exception {
    conf.set(MimeTypeIndexingFilter.MIMEFILTER_REGEX_FILE, "block-html.txt");
    filter.setConf(conf);

    for (int i = 0; i < parses.length; i++) {
      NutchDocument doc = filter.filter(new NutchDocument(), parses[i],
          new Text("http://www.example.com/"), new CrawlDatum(), new Inlinks());

      if (MIME_TYPES[i].contains("html")) {
        Assert.assertNull("Block only HTML documents", doc);
      } else {
        Assert.assertNotNull("Allow everything else", doc);
      }
    }
  }
}

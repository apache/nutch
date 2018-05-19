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
package org.apache.nutch.indexer.links;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
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

import java.net.URL;
import java.util.Iterator;

public class TestLinksIndexingFilter {

  Configuration conf = NutchConfiguration.create();
  LinksIndexingFilter filter = new LinksIndexingFilter();
  Metadata metadata = new Metadata();

  @Before
  public void setUp() throws Exception {
    metadata.add(Response.CONTENT_TYPE, "text/html");
  }

  private Outlink[] generateOutlinks() throws Exception {
    return generateOutlinks(false);
  }

  private Outlink[] generateOutlinks(boolean parts) throws Exception {
    Outlink[] outlinks = new Outlink[2];

    outlinks[0] = new Outlink("http://www.test.com", "test");
    outlinks[1] = new Outlink("http://www.example.com", "example");

    if (parts) {
      outlinks[0] = new Outlink(outlinks[0].getToUrl() + "/index.php?param=1",
          "test");
      outlinks[1] = new Outlink(outlinks[1].getToUrl() + "/index.php?param=2",
          "test");
    }

    return outlinks;
  }

  @Test
  public void testFilterOutlinks() throws Exception {
    conf.set(LinksIndexingFilter.LINKS_OUTLINKS_HOST, "true");
    filter.setConf(conf);

    Outlink[] outlinks = generateOutlinks();

    NutchDocument doc = filter.filter(new NutchDocument(), new ParseImpl("text",
            new ParseData(new ParseStatus(), "title", outlinks, metadata)),
        new Text("http://www.example.com/"), new CrawlDatum(), new Inlinks());

    Assert.assertEquals(1, doc.getField("outlinks").getValues().size());

    Assert.assertEquals("Filter outlinks, allow only those from a different host",
        outlinks[0].getToUrl(), doc.getFieldValue("outlinks"));
  }

  @Test
  public void testFilterInlinks() throws Exception {
    conf.set(LinksIndexingFilter.LINKS_INLINKS_HOST, "true");
    filter.setConf(conf);

    Inlinks inlinks = new Inlinks();
    inlinks.add(new Inlink("http://www.test.com", "test"));
    inlinks.add(new Inlink("http://www.example.com", "example"));

    NutchDocument doc = filter.filter(new NutchDocument(), new ParseImpl("text",
            new ParseData(new ParseStatus(), "title", new Outlink[0], metadata)),
        new Text("http://www.example.com/"), new CrawlDatum(), inlinks);

    Assert.assertEquals(1, doc.getField("inlinks").getValues().size());

    Assert.assertEquals("Filter inlinks, allow only those from a different host",
        "http://www.test.com", doc.getFieldValue("inlinks"));
  }

  @Test
  public void testNoFilterOutlinks() throws Exception {
    filter.setConf(conf);

    Outlink[] outlinks = generateOutlinks();

    NutchDocument doc = filter.filter(new NutchDocument(), new ParseImpl("text",
            new ParseData(new ParseStatus(), "title", outlinks, metadata)),
        new Text("http://www.example.com/"), new CrawlDatum(), new Inlinks());

    Assert.assertEquals("All outlinks must be indexed even those from the same host",
        outlinks.length, doc.getField("outlinks").getValues().size());
  }

  @Test
  public void testNoFilterInlinks() throws Exception {
    conf.set(LinksIndexingFilter.LINKS_INLINKS_HOST, "false");
    filter.setConf(conf);

    Inlinks inlinks = new Inlinks();
    inlinks.add(new Inlink("http://www.test.com", "test"));
    inlinks.add(new Inlink("http://www.example.com", "example"));

    NutchDocument doc = filter.filter(new NutchDocument(), new ParseImpl("text",
            new ParseData(new ParseStatus(), "title", new Outlink[0], metadata)),
        new Text("http://www.example.com/"), new CrawlDatum(), inlinks);

    Assert.assertEquals("All inlinks must be indexed even those from the same host",
        inlinks.size(), doc.getField("inlinks").getValues().size());
  }

  @Test
  public void testIndexOnlyHostPart() throws Exception {
    conf.set(LinksIndexingFilter.LINKS_INLINKS_HOST, "true");
    conf.set(LinksIndexingFilter.LINKS_OUTLINKS_HOST, "true");
    conf.set(LinksIndexingFilter.LINKS_ONLY_HOSTS, "true");
    filter.setConf(conf);

    Outlink[] outlinks = generateOutlinks(true);

    Inlinks inlinks = new Inlinks();
    inlinks.add(new Inlink("http://www.test.com/one-awesome-page", "test"));
    inlinks.add(new Inlink("http://www.test.com/other-awesome-page", "test"));
    inlinks.add(new Inlink("http://www.example.com/my-first-awesome-example",
        "example"));

    NutchDocument doc = filter.filter(new NutchDocument(), new ParseImpl("text",
            new ParseData(new ParseStatus(), "title", outlinks, metadata)),
        new Text("http://www.example.com/"), new CrawlDatum(), inlinks);

    NutchField docOutlinks = doc.getField("outlinks");

    Assert.assertEquals("Only the host portion of the outlink URL must be indexed",
        new URL("http://www.test.com").getHost(),
        docOutlinks.getValues().get(0));

    Assert.assertEquals(
        "The inlinks coming from the same host must count only once", 1,
        doc.getField("inlinks").getValues().size());

    Assert.assertEquals("Only the host portion of the inlinks URL must be indexed",
        new URL("http://www.test.com").getHost(), doc.getFieldValue("inlinks"));
  }

  @Test
  public void testIndexHostsOnlyAndFilterOutlinks() throws Exception {
    conf = NutchConfiguration.create();
    conf.set(LinksIndexingFilter.LINKS_ONLY_HOSTS, "true");
    conf.set(LinksIndexingFilter.LINKS_OUTLINKS_HOST, "true");

    Outlink[] outlinks = generateOutlinks(true);

    filter.setConf(conf);

    NutchDocument doc = filter.filter(new NutchDocument(), new ParseImpl("text",
            new ParseData(new ParseStatus(), "title", outlinks, metadata)),
        new Text("http://www.example.com/"), new CrawlDatum(), new Inlinks());

    Assert.assertEquals(1, doc.getField("outlinks").getValues().size());

    Assert.assertEquals(
        "Index only the host portion of the outlinks after filtering",
        new URL("http://www.test.com").getHost(),
        doc.getFieldValue("outlinks"));
  }

  @Test
  public void testIndexHostsOnlyAndFilterInlinks() throws Exception {
    conf = NutchConfiguration.create();
    conf.set(LinksIndexingFilter.LINKS_ONLY_HOSTS, "true");
    conf.set(LinksIndexingFilter.LINKS_INLINKS_HOST, "true");

    filter.setConf(conf);

    Inlinks inlinks = new Inlinks();
    inlinks.add(new Inlink("http://www.test.com", "test"));
    inlinks.add(new Inlink("http://www.example.com", "example"));

    NutchDocument doc = filter.filter(new NutchDocument(), new ParseImpl("text",
            new ParseData(new ParseStatus(), "title", new Outlink[0], metadata)),
        new Text("http://www.example.com/"), new CrawlDatum(), inlinks);

    Assert.assertEquals(1, doc.getField("inlinks").getValues().size());

    Assert.assertEquals(
        "Index only the host portion of the inlinks after filtering",
        new URL("http://www.test.com").getHost(),
        doc.getFieldValue("inlinks"));

  }
}

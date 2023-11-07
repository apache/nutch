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
package org.apache.nutch.indexer.more;

import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("more")
public class TestMoreIndexingFilter {

  @Test
  public void testContentType() throws IndexingException {
    Configuration conf = NutchConfiguration.create();
    assertContentType(conf, "text/html", "text/html");
    assertContentType(conf, "text/html; charset=UTF-8", "text/html");
  }

  @Test
  public void testGetParts() {
    String[] parts = MoreIndexingFilter.getParts("text/html");
    assertParts(parts, 2, "text", "html");
  }

  /**
   * @since NUTCH-901
   */
  @Test
  public void testNoParts() {
    Configuration conf = NutchConfiguration.create();
    conf.setBoolean("moreIndexingFilter.indexMimeTypeParts", false);
    MoreIndexingFilter filter = new MoreIndexingFilter();
    filter.setConf(conf);
    assertNotNull(filter);
    NutchDocument doc = new NutchDocument();
    ParseImpl parse = new ParseImpl("foo bar", new ParseData());

    try {
      filter.filter(doc, parse, new Text("http://nutch.apache.org/index.html"),
          new CrawlDatum(), new Inlinks());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertNotNull(doc);
    assertTrue(doc.getFieldNames().contains("type"));
    assertEquals(1, doc.getField("type").getValues().size());
    assertEquals("text/html", doc.getFieldValue("type"));
  }

  @Test
  public void testContentDispositionTitle() throws IndexingException {
    Configuration conf = NutchConfiguration.create();

    Metadata metadata = new Metadata();
    metadata.add(Response.CONTENT_DISPOSITION, "filename=filename.ext");
    MoreIndexingFilter filter = new MoreIndexingFilter();
    filter.setConf(conf);

    Text url = new Text("http://www.example.com/");
    ParseImpl parseImpl = new ParseImpl("text", new ParseData(
        new ParseStatus(), "title", new Outlink[0], metadata));

    NutchDocument doc = new NutchDocument();
    doc = filter.filter(doc, parseImpl, url, new CrawlDatum(), new Inlinks());

    assertEquals("filename.ext", doc.getFieldValue("title"),
        "content-disposition not detected");

    /* NUTCH-1140: do not add second title to avoid a multi-valued title field */
    doc = new NutchDocument();
    doc.add("title", "title");
    doc = filter.filter(doc, parseImpl, url, new CrawlDatum(), new Inlinks());
    assertEquals("title", doc.getFieldValue("title"),
        "do not add second title by content-disposition");
  }

  private void assertParts(String[] parts, int count, String... expected) {
    assertEquals(count, parts.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], parts[i]);
    }
  }

  private void assertContentType(Configuration conf, String source,
      String expected) throws IndexingException {
    Metadata metadata = new Metadata();
    metadata.add(Response.CONTENT_TYPE, source);
    MoreIndexingFilter filter = new MoreIndexingFilter();
    filter.setConf(conf);
    NutchDocument doc = filter.filter(new NutchDocument(), new ParseImpl(
        "text", new ParseData(new ParseStatus(), "title", new Outlink[0],
            metadata)), new Text("http://www.example.com/"), new CrawlDatum(),
        new Inlinks());
    assertEquals(expected, doc.getFieldValue("type"),
        "mime type not detected");
  }

  @Test
  public void testDates() throws IndexingException {
    Configuration conf = NutchConfiguration.create();

    Metadata metadata = new Metadata();
    MoreIndexingFilter filter = new MoreIndexingFilter();
    filter.setConf(conf);

    Text url = new Text("http://www.example.com/");
    ParseImpl parseImpl = new ParseImpl("text",
        new ParseData(new ParseStatus(), "title", new Outlink[0], metadata));
    CrawlDatum fetchDatum = new CrawlDatum();
    NutchDocument doc = new NutchDocument();

    long dateEpocheSeconds = 1537898340; // 2018-09-25T17:59:00+0000
    fetchDatum.setModifiedTime(dateEpocheSeconds * 1000);
    // fetch time 30 days later
    fetchDatum.setFetchTime((dateEpocheSeconds + 30 * 24 * 60 * 60) * 1000);
    // NOTE: the datum.getLastModified() returns the fetch time
    // of the latest fetch of changed content (i.e., not a "HTTP 304
    // not-modified" or a re-fetch resulting in the same signature)

    doc = filter.filter(doc, parseImpl, url, fetchDatum, new Inlinks());

    assertEquals(new Date(dateEpocheSeconds * 1000),
        doc.getFieldValue("date"),
        "last fetch date not extracted");

    // set last-modified time (7 days before fetch time)
    Date lastModifiedDate = new Date(
        (dateEpocheSeconds - 7 * 24 * 60 * 60) * 1000);
    String lastModifiedDateStr = DateTimeFormatter.ISO_INSTANT.format(lastModifiedDate.toInstant());
    parseImpl.getData().getParseMeta().set(Metadata.LAST_MODIFIED, lastModifiedDateStr);
    doc = filter.filter(doc, parseImpl, url, fetchDatum, new Inlinks());

    assertEquals(lastModifiedDate, doc.getFieldValue("lastModified"),
        "last-modified date not extracted");
  }
}

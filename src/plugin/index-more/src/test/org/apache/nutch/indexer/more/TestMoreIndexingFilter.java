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

import junit.framework.TestCase;

public class TestMoreIndexingFilter extends TestCase {

  public void testContentType() throws IndexingException {
    Configuration conf = NutchConfiguration.create();
    assertContentType(conf, "text/html", "text/html");
    assertContentType(conf, "text/html; charset=UTF-8", "text/html");
  }
  
  public void testGetParts() {
    String[] parts = MoreIndexingFilter.getParts("text/html");
    assertParts(parts, 2, "text", "html");

  }

    /**
     * @since NUTCH-901
     */
    public void testNoParts(){
	Configuration conf = NutchConfiguration.create();
	conf.setBoolean("moreIndexingFilter.indexMimeTypeParts", false);
	MoreIndexingFilter filter = new MoreIndexingFilter();
	filter.setConf(conf);
	assertNotNull(filter);
	NutchDocument doc = new NutchDocument();
        ParseImpl parse = new ParseImpl("foo bar", new ParseData());
        
	try{
	    filter.filter(doc, parse, new Text("http://nutch.apache.org/index.html"), new CrawlDatum(), new Inlinks());
	}
        catch(Exception e){
	    e.printStackTrace();
	    fail(e.getMessage());
	}
	assertNotNull(doc);
	assertTrue(doc.getFieldNames().contains("type"));
	assertEquals(1, doc.getField("type").getValues().size());
	assertEquals("text/html", doc.getFieldValue("type"));     
    }

  private void assertParts(String[] parts, int count, String... expected) {
    assertEquals(count, parts.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], parts[i]);
    }
  }
  
  private void assertContentType(Configuration conf, String source, String expected) throws IndexingException {
    Metadata metadata = new Metadata();
    metadata.add(Response.CONTENT_TYPE, source);
    MoreIndexingFilter filter = new MoreIndexingFilter();
    filter.setConf(conf);
    NutchDocument doc = filter.filter(new NutchDocument(), new ParseImpl("text", new ParseData(
        new ParseStatus(), "title", new Outlink[0], metadata)), new Text(
        "http://www.example.com/"), new CrawlDatum(), new Inlinks());
    assertEquals("mime type not detected", expected, doc.getFieldValue("type"));
  }
}

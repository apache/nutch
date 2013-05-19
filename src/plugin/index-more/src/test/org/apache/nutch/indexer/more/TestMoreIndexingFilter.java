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

import java.nio.ByteBuffer;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.EncodingDetector;
import org.apache.nutch.util.NutchConfiguration;

public class TestMoreIndexingFilter {

  @Test
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
  @Test
  public void testNoParts(){
     Configuration conf = NutchConfiguration.create();
     conf.setBoolean("moreIndexingFilter.indexMimeTypeParts", false);
     MoreIndexingFilter filter = new MoreIndexingFilter();
     filter.setConf(conf);
     assertNotNull(filter);
     NutchDocument doc = new NutchDocument();
     try{
       filter.filter(doc, "http://nutch.apache.org/index.html", new WebPage());
     }
     catch(Exception e){
       e.printStackTrace();
       fail(e.getMessage());
     }
     assertNotNull(doc);
     assertTrue(doc.getFieldNames().contains("type"));
     assertEquals(1, doc.getFieldValues("type").size());
     assertEquals("text/html", doc.getFieldValue("type"));     
  }
  
  private void assertParts(String[] parts, int count, String... expected) {
    assertEquals(count, parts.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], parts[i]);
    }
  }
  
  private void assertContentType(Configuration conf, String source, String expected) throws IndexingException {
    MoreIndexingFilter filter = new MoreIndexingFilter();
    filter.setConf(conf);
    WebPage page = new WebPage();
    String url = "http://www.example.com/";
    page.setContent(ByteBuffer.wrap("text".getBytes()));
    page.setTitle(new Utf8("title"));
    page.putToHeaders(EncodingDetector.CONTENT_TYPE_UTF8, new Utf8(source));
    NutchDocument doc = filter.filter(new NutchDocument(), url, page);
    assertEquals("mime type not detected", expected, doc.getFieldValue("type"));
  }
}

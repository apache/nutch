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
package org.apache.nutch.microformats.reltag;

import java.nio.ByteBuffer;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *JUnit test case for {@link RelTagIndexingFilter} which 
 *simply asserts that a 'tag' field is obtained by the filter.
 *
 *@author lewismc
 */

  public class TestRelTagIndexingFilter {

  @Test
  public void testRelTagFields() throws Exception {
    Configuration conf = NutchConfiguration.create();
    RelTagIndexingFilter filter = new RelTagIndexingFilter();
    filter.setConf(conf);
    assertNotNull(filter);
    NutchDocument doc = new NutchDocument();
    WebPage page = new WebPage();
    byte[] bytes = new byte[10];
    ByteBuffer bbuf = ByteBuffer.wrap(bytes);
    page.putToMetadata(new Utf8(RelTagParser.REL_TAG), bbuf);
    try {
      filter.filter(doc, "http://nutch.apache.org/", page);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertNotNull(doc);
    assertTrue("check for 'tag' field", doc.getFieldNames().contains("tag"));
  }
}
  
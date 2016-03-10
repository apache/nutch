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
package org.apache.nutch.parse.xsl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;

import org.junit.Test;

/**
 * 
 * Testing the filter that will auto import fields defined in the xsl file.
 * 
 */
public class TestXslIndexFilter extends AbstractCrawlTest {

  /**
   * Test the fields fetch from xsl file.
   */
  @Test
  public void testFields() {
    XslIndexFilter filter = new XslIndexFilter();
    try {
      List<String> list = filter.extractFields(
          new File(sampleDir, "sample1/transformer_book.xsl").toString());
      assertNotNull(list);
      assertEquals(6, list.size());
    } catch (Exception e) {
      fail();
    }
  }
}

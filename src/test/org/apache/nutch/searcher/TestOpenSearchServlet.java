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
package org.apache.nutch.searcher;

import junit.framework.TestCase;

public class TestOpenSearchServlet extends TestCase {

  /**
   * Test removing of illegal xml chars from string
   */
  public void testGetLegalXml(){
    assertEquals("hello",OpenSearchServlet.getLegalXml("hello"));
    assertEquals("hello",OpenSearchServlet.getLegalXml("he\u0000llo"));
    assertEquals("hello",OpenSearchServlet.getLegalXml("\u0000he\u0000llo"));
    assertEquals("hello",OpenSearchServlet.getLegalXml("\u0000he\u0000llo\u0000"));
  }
  
  /**
   * Test turning Lucene column names into valid XML names.
   * 
   * The Nutch FAQ explains that OpenSearch includes "all fields that are available
   * at search result time." However, some Lucene column names can start
   * with numbers. Valid XML tags cannot. If Nutch is generating OpenSearch results
   * for a document with a Lucene document column whose name starts with numbers,
   * the underlying Xerces library throws this exception:
   * 
   *  
   * org.w3c.dom.DOMException: INVALID_CHARACTER_ERR: An invalid or illegal XML character is specified. 
   * 
   * Therefore, we test here that Nutch can turn strings into valid XML tags.
   */
  public void testGetLegalTagName(){
	  assertEquals("nutch_000_numbers_first", OpenSearchServlet.getLegalTagName("000_numbers_first"));
	  assertEquals("letters_first_000", OpenSearchServlet.getLegalTagName("letters_first_000"));
  }
  

}

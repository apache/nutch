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
  
}

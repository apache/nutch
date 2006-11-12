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

package org.apache.nutch.parse;

// JUnit imports
import junit.framework.TestCase;

// Nutch imports
import org.apache.nutch.plugin.Extension;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

/**
 * Unit test for new parse plugin selection.
 *
 * @author Sebastien Le Callonnec
 * @version 1.0
 */
public class TestParserFactory extends TestCase {
	
  private Configuration conf;
  private ParserFactory parserFactory;
    
  public TestParserFactory(String name) { super(name); }

  /** Inits the Test Case with the test parse-plugin file */
  protected void setUp() throws Exception {
      conf = NutchConfiguration.create();
      conf.set("plugin.includes", ".*");
      conf.set("parse.plugin.file",
               "org/apache/nutch/parse/parse-plugin-test.xml");
      parserFactory = new ParserFactory(conf);
  }
    
  /** Unit test for <code>getExtensions(String)</code> method. */
  public void testGetExtensions() throws Exception {
    Extension ext = (Extension)parserFactory.getExtensions("text/html").get(0);
    assertEquals("parse-html", ext.getDescriptor().getPluginId());
    ext = (Extension) parserFactory.getExtensions("text/html; charset=ISO-8859-1").get(0);
    assertEquals("parse-html", ext.getDescriptor().getPluginId());
    ext = (Extension)parserFactory.getExtensions("foo/bar").get(0);
    assertEquals("parse-text", ext.getDescriptor().getPluginId());
  }
  
  /** Unit test to check <code>getParsers</code> method */
  public void testGetParsers() throws Exception {
    Parser [] parsers = parserFactory.getParsers("text/html", "http://foo.com");
    assertNotNull(parsers);
    assertEquals(1, parsers.length);
    assertEquals("org.apache.nutch.parse.html.HtmlParser",
                 parsers[0].getClass().getName());

    parsers = parserFactory.getParsers("text/html; charset=ISO-8859-1",
                                       "http://foo.com");
    assertNotNull(parsers);
    assertEquals(1, parsers.length);
    assertEquals("org.apache.nutch.parse.html.HtmlParser",
                 parsers[0].getClass().getName());
    
    parsers = parserFactory.getParsers("application/x-javascript",
                                       "http://foo.com");
    assertNotNull(parsers);
    assertEquals(1, parsers.length);
    assertEquals("org.apache.nutch.parse.js.JSParseFilter",
                 parsers[0].getClass().getName());
    
    parsers = parserFactory.getParsers("text/plain", "http://foo.com");
    assertNotNull(parsers);
    assertEquals(1, parsers.length);
    assertEquals("org.apache.nutch.parse.text.TextParser",
                 parsers[0].getClass().getName());
    
    Parser parser1 = parserFactory.getParsers("text/plain", "http://foo.com")[0];
    Parser parser2 = parserFactory.getParsers("*", "http://foo.com")[0];
   
    assertEquals("Different instances!", parser1.hashCode(), parser2.hashCode());
    
    //test and make sure that the rss parser is loaded even though its plugin.xml
    //doesn't claim to support text/rss, only application/rss+xml
    parsers = parserFactory.getParsers("text/rss","http://foo.com");
    assertNotNull(parsers);
    assertEquals(1,parsers.length);
    assertEquals("org.apache.nutch.parse.rss.RSSParser",
                 parsers[0].getClass().getName());
  }
 
}

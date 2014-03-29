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

// Nutch imports
import org.apache.nutch.plugin.Extension;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for new parse plugin selection.
 *
 * @author Sebastien Le Callonnec
 * @version 1.0
 */
public class TestParserFactory {
	
  private Configuration conf;
  private ParserFactory parserFactory;

  /** Inits the Test Case with the test parse-plugin file */
  @Before
  public void setUp() throws Exception {
      conf = NutchConfiguration.create();
      conf.set("plugin.includes", ".*");
      conf.set("parse.plugin.file",
               "org/apache/nutch/parse/parse-plugin-test.xml");
      parserFactory = new ParserFactory(conf);
  }
    
  /** Unit test for <code>getExtensions(String)</code> method. */
  @Test
  public void testGetExtensions() throws Exception {
    Extension ext = parserFactory.getExtensions("text/html").get(0);
    Assert.assertEquals("parse-tika", ext.getDescriptor().getPluginId());
    ext = parserFactory.getExtensions("text/html; charset=ISO-8859-1").get(0);
    Assert.assertEquals("parse-tika", ext.getDescriptor().getPluginId());
    ext = parserFactory.getExtensions("foo/bar").get(0);
    Assert.assertEquals("parse-tika", ext.getDescriptor().getPluginId());
  }
  
  /** Unit test to check <code>getParsers</code> method */
  @Test
  public void testGetParsers() throws Exception {
    Parser [] parsers = parserFactory.getParsers("text/html", "http://foo.com");
    Assert.assertNotNull(parsers);
    Assert.assertEquals(1, parsers.length);
    Assert.assertEquals("org.apache.nutch.parse.tika.TikaParser",
                 parsers[0].getClass().getName());

    parsers = parserFactory.getParsers("text/html; charset=ISO-8859-1",
                                       "http://foo.com");
    Assert.assertNotNull(parsers);
    Assert.assertEquals(1, parsers.length);
    Assert.assertEquals("org.apache.nutch.parse.tika.TikaParser",
                 parsers[0].getClass().getName());
    
    parsers = parserFactory.getParsers("application/x-javascript",
                                       "http://foo.com");
    Assert.assertNotNull(parsers);
    Assert.assertEquals(1, parsers.length);
    Assert.assertEquals("org.apache.nutch.parse.js.JSParseFilter",
                 parsers[0].getClass().getName());
    
    parsers = parserFactory.getParsers("text/plain", "http://foo.com");
    Assert.assertNotNull(parsers);
    Assert.assertEquals(1, parsers.length);
    Assert.assertEquals("org.apache.nutch.parse.tika.TikaParser",
                 parsers[0].getClass().getName());
    
    Parser parser1 = parserFactory.getParsers("text/plain", "http://foo.com")[0];
    Parser parser2 = parserFactory.getParsers("*", "http://foo.com")[0];
   
    Assert.assertEquals("Different instances!", parser1.hashCode(), parser2.hashCode());
    
    //test and make sure that the rss parser is loaded even though its plugin.xml
    //doesn't claim to support text/rss, only application/rss+xml
    parsers = parserFactory.getParsers("text/rss","http://foo.com");
    Assert.assertNotNull(parsers);
    Assert.assertEquals(1,parsers.length);
    Assert.assertEquals("org.apache.nutch.parse.tika.TikaParser",
                 parsers[0].getClass().getName());
  }
 
}

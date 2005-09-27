/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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


/**
 * Unit test for new parse plugin selection.
 *
 * @author Sebastien Le Callonnec
 * @version 1.0
 */
public class TestParserFactory extends TestCase {
  
  public TestParserFactory(String name) { super(name); }
  
  
  /** Unit test for <code>getParser(String, String)</code> method. */
  public void testGetParser() throws Exception {
    Parser  parser = ParserFactory.getParser("text/html", "http://foo.com/");
    assertNotNull(parser);
    parser  = ParserFactory.getParser("foo/bar", "http://foo.com/");
    assertNotNull(parser);
  }
  
  /** Unit test for <code>getExtension(String)</code> method. */
  public void testGetExtension() throws Exception {
    Extension ext = ParserFactory.getExtension("text/html");
    assertEquals("parse-html", ext.getDescriptor().getPluginId());
    ext = ParserFactory.getExtension("foo/bar");
    assertEquals("parse-text", ext.getDescriptor().getPluginId());
  }
  
}

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
package org.apache.nutch.keymatch;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class TestSimpleKeyMatcher extends TestCase {

  
  /* (non-Javadoc)
   * @see junit.framework.TestCase#setUp()
   */
  protected void setUp() throws Exception {
    super.setUp();
    conf=NutchConfiguration.create();
    km=new SimpleKeyMatcher(conf);
  }
  
  SimpleKeyMatcher km;
  
  Configuration conf;

  /*
   * Test method for 'org.apache.nutch.keymatch.SimpleKeyMatcher.getMatches(Query, int)'
   */
  public void testGetMatches() {
    HashMap context=new HashMap();
    context.put(CountFilter.KEY_COUNT,"1");
    
    //keyword
    KeyMatch[] matches=getKeyMatchesForString("kw1 kw2 kw3 auto");
    assertEquals(1,matches.length);
    
    //phrase
    matches=getKeyMatchesForString("search engine");
    assertEquals(1,matches.length);
    
    //exact + phrase
    matches=getKeyMatchesForString("apache search engine");
    assertEquals(2,matches.length);

    //exact
    matches=getKeyMatchesForString("kw1 kw2 kw3 kw4");
    assertEquals(1,matches.length);

    matches=getKeyMatchesForString("kw2 kw2 kw3 kw4");
    assertEquals(0,matches.length);

  }

  /*
   * Test method for 'org.apache.nutch.keymatch.SimpleKeyMatcher.addKeyMatch(Map, KeyMatch, boolean)'
   */
  public void testAddKeyMatch() {
    KeyMatch keymatch=new KeyMatch("httpd","http://www.apache.org/","apache", KeyMatch.TYPE_KEYWORD);
    km.addKeyMatch(keymatch);
    KeyMatch matched[]=getKeyMatchesForString("httpd");
    assertTrue(keymatch.equals(matched[0]));
  }

  private KeyMatch[] getKeyMatchesForString(String string) {
    
    Query q;
    HashMap context=new HashMap();
    context.put(CountFilter.KEY_COUNT,"1");
    try {
      q = Query.parse(string, conf);
      return km.getMatches(q,context);
    } catch (Exception e){
      
    }
    return new KeyMatch[0];
  }
}

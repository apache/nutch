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

public class TestViewCountSorter extends TestCase {

  SimpleKeyMatcher km;
  Configuration conf;
  
  /* (non-Javadoc)
   * @see junit.framework.TestCase#setUp()
   */
  protected void setUp() throws Exception {
    super.setUp();
    conf=NutchConfiguration.create();
    km=new SimpleKeyMatcher(conf);
    km.clear();
    KeyMatch m=new KeyMatch("kw1","u1","t1",KeyMatch.TYPE_KEYWORD);
    km.addKeyMatch(m);
    m=new KeyMatch("kw1","u2","t2",KeyMatch.TYPE_KEYWORD);
    km.addKeyMatch(m);
    m=new KeyMatch("kw1","u3","t3",KeyMatch.TYPE_KEYWORD);
    km.addKeyMatch(m);
    ViewCountSorter vcs=new ViewCountSorter();
    vcs.setNext(new CountFilter());
    km.setFilter(vcs);
  }

  /*
   * Test method for 'org.apache.nutch.keymatch.ViewCountSorter.filter(List, Map)'
   */
  public void testFilter() {
    KeyMatch m1,m2,m3;
    
    KeyMatch[] matches=getKeyMatchesForString("kw1");
    m1=matches[0];
    assertNotNull(m1);

    matches=getKeyMatchesForString("kw1");
    m2=matches[0];
    assertNotNull(m2);
    
    matches=getKeyMatchesForString("kw1");
    m3=matches[0];
    assertNotNull(m3);
    
    assertFalse(m1.equals(m2));
    assertFalse(m2.equals(m3));
    assertFalse(m1.equals(m3));
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

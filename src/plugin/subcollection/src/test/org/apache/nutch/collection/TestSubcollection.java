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
package org.apache.nutch.collection;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collection;

import org.apache.nutch.util.NutchConfiguration;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestSubcollection {
  
  /**Test filtering logic
   * 
   * @throws Exception
   */
  @Test
  public void testFilter() throws Exception {
    Subcollection sc=new Subcollection(NutchConfiguration.create());
    sc.setWhiteList("www.nutch.org\nwww.apache.org");
    sc.setBlackList("jpg\nwww.apache.org/zecret/");
    
    //matches whitelist
    assertEquals("http://www.apache.org/index.html", sc.filter("http://www.apache.org/index.html"));
    
    //matches blacklist
    assertEquals(null, sc.filter("http://www.apache.org/zecret/index.html"));
    assertEquals(null, sc.filter("http://www.apache.org/img/image.jpg"));
    
    //no match
    assertEquals(null, sc.filter("http://www.google.com/"));
  }
  
  @Test
  public void testInput(){
    StringBuffer xml=new StringBuffer();
    xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    xml.append("<!-- just a comment -->");
    xml.append("<subcollections>");
    xml.append("<subcollection>");
    xml.append("<name>nutch collection</name>");
    xml.append("<id>nutch</id>");
    xml.append("<whitelist>");
    xml.append("http://lucene.apache.org/nutch/\n");
    xml.append("http://wiki.apache.org/nutch/\n");
    xml.append("</whitelist>");
    xml.append("<blacklist>");
    xml.append("http://www.xxx.yyy\n");
    xml.append("</blacklist>");
    xml.append("</subcollection>");
    xml.append("</subcollections>");
    
    InputStream is=new ByteArrayInputStream(xml.toString().getBytes());
    
    CollectionManager cm=new CollectionManager();
    cm.parse(is);
    
    Collection c=cm.getAll();
    
    // test that size matches
    assertEquals(1,c.size());
    
    Subcollection collection=(Subcollection)c.toArray()[0];
    
    //test collection id
    assertEquals("nutch", collection.getId());
    
    //test collection name
    assertEquals("nutch collection", collection.getName());

    //test whitelist
    assertEquals(2,collection.whiteList.size());
    
    String wlUrl=(String)collection.whiteList.get(0);
    assertEquals("http://lucene.apache.org/nutch/", wlUrl);

    wlUrl=(String)collection.whiteList.get(1);
    assertEquals("http://wiki.apache.org/nutch/", wlUrl);
    
    //matches whitelist
    assertEquals("http://lucene.apache.org/nutch/", collection.filter("http://lucene.apache.org/nutch/"));

    //test blacklist
    assertEquals(1,collection.blackList.size());

    String blUrl=(String)collection.blackList.get(0);
    assertEquals("http://www.xxx.yyy", blUrl);

    //no match
    assertEquals(null, collection.filter("http://www.google.com/"));
  }
}

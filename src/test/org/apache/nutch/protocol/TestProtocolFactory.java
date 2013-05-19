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
package org.apache.nutch.protocol;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.ObjectCache;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestProtocolFactory {

  Configuration conf;
  ProtocolFactory factory;
  
  @Before
  public void setUp() throws Exception {
    conf = NutchConfiguration.create();
    conf.set("plugin.includes", ".*");
    conf.set("http.agent.name", "test-bot");
    factory=new ProtocolFactory(conf);
  }

  @Test
  public void testGetProtocol(){

    //non existing protocol
    try {
      factory.getProtocol("xyzxyz://somehost");
      fail("Must throw ProtocolNotFound");
    } catch (ProtocolNotFound e) {
      //all is ok
    } catch (Exception ex){
      fail("Must not throw any other exception");
    }
    
    Protocol httpProtocol=null;
    
    //existing protocol
    try {
      httpProtocol=factory.getProtocol("http://somehost");
      assertNotNull(httpProtocol);
    } catch (Exception ex){
      fail("Must not throw any other exception");
    }

    //cache key
    Object protocol = ObjectCache.get(conf).getObject(Protocol.X_POINT_ID + "http");
    assertNotNull(protocol);
    assertEquals(httpProtocol, protocol);
    
    //test same object instance
    try {
      assertTrue(httpProtocol==factory.getProtocol("http://somehost"));
    } catch (ProtocolNotFound e) {
      fail("Must not throw any exception");
    }
  }
  
  @Test
  public void testContains(){
    assertTrue(factory.contains("http", "http"));
    assertTrue(factory.contains("http", "http,ftp"));
    assertTrue(factory.contains("http", "   http ,   ftp"));
    assertTrue(factory.contains("smb", "ftp,smb,http"));
    assertFalse(factory.contains("smb", "smbb"));
  }
  
}

/*
 * Copyright 2006 The Apache Software Foundation
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
package org.apache.nutch.protocol;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class TestProtocolFactory extends TestCase {

  Configuration conf;
  
  protected void setUp() throws Exception {
    conf = NutchConfiguration.create();
    conf.set("plugin.includes", ".*");
    conf.set("http.agent.name", "test-bot");
  }

  public void testGetProtocol(){
    ProtocolFactory factory=new ProtocolFactory(conf);

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
    Object protocol=conf.getObject(Protocol.X_POINT_ID + "http");
    assertNotNull(protocol);
    assertEquals(httpProtocol, protocol);
    
    //test same object instance
    try {
      assertTrue(httpProtocol==factory.getProtocol("http://somehost"));
    } catch (ProtocolNotFound e) {
      fail("Must not throw any exception");
    }
  }
  
}

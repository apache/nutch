package org.apache.nutch.protocol;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class TestProtocolFactory extends TestCase {

  public void testGetProtocol(){
    Configuration conf=NutchConfiguration.create();
    
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
      System.out.println(httpProtocol);
      System.out.println(factory.getProtocol("http://somehost"));
      assertTrue(httpProtocol==factory.getProtocol("http://somehost"));
    } catch (ProtocolNotFound e) {
      fail("Must not throw any exception");
    }
  }
  
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestProtocolFactory {

  Configuration conf;
  ProtocolFactory factory;

  @BeforeEach
  public void setUp() throws Exception {
    conf = NutchConfiguration.create();
    conf.set("plugin.includes", ".*");
    conf.set("http.agent.name", "test-bot");
    factory = new ProtocolFactory(conf);
  }

  @Test
  public void testGetProtocol() {

    // non existing protocol
    try {
      factory.getProtocol("xyzxyz://somehost");
      fail("Must throw ProtocolNotFound");
    } catch (ProtocolNotFound e) {
      // all is ok
    } catch (Exception ex) {
      fail("Must not throw any other exception");
    }

    Protocol httpProtocol = null;

    // existing protocol
    try {
      httpProtocol = factory.getProtocol("http://somehost");
      assertNotNull(httpProtocol);
    } catch (Exception ex) {
      fail("Must not throw any other exception");
    }

    // test same object instance
    try {
      assertSame(httpProtocol, factory.getProtocol("http://somehost"));
    } catch (ProtocolNotFound e) {
      fail("Must not throw any exception");
    }
  }

  @Test
  public void testContains() {
    assertTrue(factory.contains("http", "http"));
    assertTrue(factory.contains("http", "http,ftp"));
    assertTrue(factory.contains("http", "   http ,   ftp"));
    assertTrue(factory.contains("smb", "ftp,smb,http"));
    assertFalse(factory.contains("smb", "smbb"));
  }

  @Test
  public void testProtocolMapping() throws ProtocolNotFound {
    // test mappings defined in src/test/host-protocol-mapping.txt
    // (copied into and loaded from test classpath build/test/classes)
    Protocol defaultHttpProtocol = factory.getProtocol("http://example.org/");
    assertEquals("org.apache.nutch.protocol.http.Http",
        defaultHttpProtocol.getClass().getCanonicalName());
    Protocol defaultHttpsProtocol = factory.getProtocol("https://example.org/");
    assertEquals("org.apache.nutch.protocol.okhttp.OkHttp",
        defaultHttpsProtocol.getClass().getCanonicalName());
    Protocol nutchProtocol = factory.getProtocol("https://nutch.apache.org/");
    assertSame(defaultHttpProtocol, nutchProtocol);
    Protocol squareProtocol = factory
        .getProtocol("https://square.github.io/okhttp/");
    assertSame(defaultHttpsProtocol, squareProtocol);
    Protocol tikaProtocol = factory.getProtocol("https://tika.apache.org/");
    assertEquals("org.apache.nutch.protocol.httpclient.Http",
        tikaProtocol.getClass().getCanonicalName());
    Protocol seleniumProtocol = factory
        .getProtocol("https://www.selenium.dev/");
    assertEquals("org.apache.nutch.protocol.selenium.Http",
        seleniumProtocol.getClass().getCanonicalName());
  }
}

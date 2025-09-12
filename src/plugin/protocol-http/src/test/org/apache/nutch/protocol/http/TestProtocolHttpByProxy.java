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
package org.apache.nutch.protocol.http;

import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.AbstractHttpProtocolPluginTest;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for protocol-http by proxy
 */
public class TestProtocolHttpByProxy extends AbstractHttpProtocolPluginTest {

  public static final String PROXY_HOST = "localhost";
  public static final Integer PROXY_PORT = 8888;

  public static final String TARGET_HOST = "www.baidu.com";
  public static final Integer TARGET_PORT = 443;

  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
    conf.set("http.proxy.host", PROXY_HOST);
    conf.set("http.proxy.port", PROXY_PORT.toString());
    http.setConf(conf);

    HttpProxyServer server = DefaultHttpProxyServer.bootstrap()
        .withPort(PROXY_PORT).start();
  }

  @Override
  protected String getPluginClassName() {
    return "org.apache.nutch.protocol.http.Http";
  }

  @Test
  public void testRequestByProxy() throws Exception {
    Http httpObj = new Http();
    httpObj.setConf(conf);

    String url = "https://" + TARGET_HOST;
    ProtocolOutput out = httpObj.getProtocolOutput(new Text(url),
        new CrawlDatum());
    assertNotNull(out);

    ProtocolStatus status = out.getStatus();
    assertNotNull(status);
    assertTrue(status.isSuccess());

    Content content = out.getContent();
    assertNotNull(content);
    assertTrue(content.toString().length() > 250);
  }
}

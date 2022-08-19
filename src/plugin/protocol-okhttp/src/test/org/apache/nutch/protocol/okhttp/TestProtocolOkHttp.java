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
package org.apache.nutch.protocol.okhttp;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Map;
import java.util.TreeMap;

import org.apache.nutch.protocol.AbstractHttpProtocolPluginTest;
import org.junit.Test;

/**
 * Test cases for protocol-okhttp
 */
public class TestProtocolOkHttp extends AbstractHttpProtocolPluginTest {

  @Override
  protected String getPluginClassName() {
    return "org.apache.nutch.protocol.okhttp.OkHttp";
  }

  @Test
  public void testStatusCode() throws Exception {
    Map<String, byte[]> responses = new TreeMap<>();
    responses.put("/basic-http.jsp",
        (responseHeader + simpleContent).getBytes(UTF_8));
    responses.put("/redirect301.jsp", redirect301.getBytes(UTF_8));
    responses.put("/redirect302.jsp", redirect302.getBytes(UTF_8));
    responses.put("/brokenpage.jsp", serverError.getBytes(UTF_8));
    launchServer(responses);

    fetchPage("/basic-http.jsp", 200, "text/html");
    fetchPage("/redirect301.jsp", 301);
    fetchPage("/redirect302.jsp", 302);
    fetchPage("/nonexists.html", 404);
    fetchPage("/brokenpage.jsp", 500);
  }

}

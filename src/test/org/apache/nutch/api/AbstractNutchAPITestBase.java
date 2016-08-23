/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.api.impl.RAMConfManager;
import org.apache.nutch.api.security.AuthenticationTypeEnum;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.BeforeClass;
import org.restlet.data.ChallengeRequest;
import org.restlet.data.ChallengeResponse;
import org.restlet.data.ChallengeScheme;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;

import static org.junit.Assert.assertEquals;

/**
 * Base class for Nutc REST API tests.
 * Default path is /admin and default port is 8081.
 */
public abstract class AbstractNutchAPITestBase {
  protected static Configuration conf;
  protected static final Integer DEFAULT_PORT = 8081;
  protected static final String DEFAULT_PATH = "/admin";
  public NutchServer nutchServer;

  /**
   * Creates Nutch configuration
   *
   * @throws Exception
   */
  @BeforeClass
  public static void before() throws Exception {
    conf = NutchConfiguration.create();
  }

  /**
   * Tests a request for given expected status code for {@link NutchServer} from which port it runs
   *
   * @param expectedStatusCode expected status code
   * @param port port that {@link NutchServer} runs at
   */
  public void testRequest(int expectedStatusCode, int port) {
    testRequest(expectedStatusCode, port, null, null, DEFAULT_PATH, null);
  }

  /**
   * Tests a request for given expected status code for {@link NutchServer} from which port it runs
   * with given username and password credentials
   *
   * @param expectedStatusCode expected status code
   * @param port port that {@link NutchServer} runs at
   * @param username username
   * @param password password
   */
  public void testRequest(int expectedStatusCode, int port, String username, String password) {
    testRequest(expectedStatusCode, port, username, password, DEFAULT_PATH, null);
  }

  /**
   * Tests a request for given expected status code for {@link NutchServer} from which port it runs
   * with given username and password credentials for a {@link ChallengeScheme}
   *
   * @param expectedStatusCode expected status code
   * @param port port that {@link NutchServer} runs at
   * @param username username
   * @param password password
   * @param challengeScheme challenge scheme
   */
  public void testRequest(int expectedStatusCode, int port, String username, String password, ChallengeScheme challengeScheme) {
    testRequest(expectedStatusCode, port, username, password, DEFAULT_PATH, challengeScheme);
  }

  /**
   * Tests a request for given expected status code for {@link NutchServer} from which port it runs
   * with given username and password credentials for a {@link ChallengeScheme}
   *
   * @param expectedStatusCode expected status code
   * @param port port that {@link NutchServer} runs at
   * @param username username
   * @param password password
   * @param path path
   * @param challengeScheme challenge scheme
   */
  public void testRequest(int expectedStatusCode, int port, String username, String password, String path,
                          ChallengeScheme challengeScheme) {
    if (port <= 0) {
      port = DEFAULT_PORT;
    }

    if (!path.startsWith("/")) {
      path += "/";
    }
    String protocol = AuthenticationTypeEnum.SSL.toString().equals(conf.get("restapi.auth")) ? "https" : "http";
    ClientResource resource = new ClientResource(protocol + "://localhost:" + port + path);
    if (challengeScheme != null) {
      resource.setChallengeResponse(challengeScheme, username, password);
    }

    try {
      resource.get();
    } catch (ResourceException rex) {
      //Don't use it. Use the status code to check afterwards
    }

    /*
    If request was a HTTP Digest request, previous request was 401. Client needs some data sent by the server so
    we should complete the ChallengeResponse.
    */

    if (ChallengeScheme.HTTP_DIGEST.equals(challengeScheme)) {
      // User server's data to complete the challengeResponse object
      ChallengeRequest digestChallengeRequest = retrieveDigestChallengeRequest(resource);
      ChallengeResponse challengeResponse = new ChallengeResponse(digestChallengeRequest, resource.getResponse(),
          username, password.toCharArray());
      testDigestRequest(expectedStatusCode, resource, challengeResponse);
    }
    assertEquals(expectedStatusCode, resource.getStatus().getCode());
  }

  private void testDigestRequest(int expectedStatusCode, ClientResource resource, ChallengeResponse challengeResponse){
    resource.setChallengeResponse(challengeResponse);
    try {
      resource.get();
    } catch (ResourceException rex) {
      //Don't use it. Use the status code to check afterwards
    }
    assertEquals(expectedStatusCode, resource.getStatus().getCode());
  }

  private ChallengeRequest retrieveDigestChallengeRequest (ClientResource resource) {
    ChallengeRequest digestChallengeRequest = null;
    for (ChallengeRequest cr : resource.getChallengeRequests()) {
      if (ChallengeScheme.HTTP_DIGEST.equals(cr.getScheme())) {
        digestChallengeRequest = cr;
        break;
      }
    }
    return digestChallengeRequest;
  }

  private void initializeSSLProperties() {
    conf.set("restapi.auth.ssl.storepath", "src/test/nutch-ssl.keystore.jks");
    conf.set("restapi.auth.ssl.storepass", "password");
    conf.set("restapi.auth.ssl.keypass", "password");
  }

  /**
   * Starts the server with given authentication type
   *
   * @param authenticationType authentication type
   */
  public void startServer(AuthenticationTypeEnum authenticationType) {
    conf.set("restapi.auth", authenticationType.toString());
    if(AuthenticationTypeEnum.SSL.equals(authenticationType)) {
      initializeSSLProperties();
    }

    RAMConfManager ramConfManager = new RAMConfManager(NutchConfiguration.getUUID(conf), conf);
    nutchServer = new NutchServer(ramConfManager, NutchConfiguration.getUUID(conf));
    nutchServer.start();
  }

  /**
   * Stops server
   */
  public void stopServer() {
    nutchServer.stop(true);
  }

}

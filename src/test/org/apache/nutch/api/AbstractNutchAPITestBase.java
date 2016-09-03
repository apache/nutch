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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileNotFoundException;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
      // Use server's data to complete the challengeResponse object
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

  private void initializeSSLProperties() throws FileNotFoundException {
    conf.set("restapi.auth.ssl.storepath", getResourcePath("nutch-ssl.keystore.jks"));
    conf.set("restapi.auth.ssl.storepass", "password");
    conf.set("restapi.auth.ssl.keypass", "password");
  }

  private void loadCertificate() throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
    String testTrustKeyStorePath = getResourcePath("testTrustKeyStore");
    File file = new File(testTrustKeyStorePath);
    InputStream localCertIn = new FileInputStream(file);

    char[] password = "testpassword".toCharArray();
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(localCertIn, password);

    localCertIn.close();

    if (ks.containsAlias("nutch")) {
      return;
    }

    String cerFilePath = getResourcePath("nutch.cer");

    InputStream certIn = new FileInputStream(cerFilePath);
    BufferedInputStream bis = new BufferedInputStream(certIn);
    CertificateFactory cf = CertificateFactory.getInstance("X.509");

    while (bis.available() > 0) {
      Certificate cert = cf.generateCertificate(bis);
      ks.setCertificateEntry("nutch", cert);
    }

    certIn.close();
    OutputStream out = new FileOutputStream(file);
    ks.store(out, password);
    out.close();
  }

  /**
   * Starts the server with given authentication type.
   * Use {@link #startSSLServer()} for a {@link AuthenticationTypeEnum#SSL} authentication type server.
   *
   * @param authenticationType authentication type
   */
  public void startServer(AuthenticationTypeEnum authenticationType) {
    conf.set("restapi.auth", authenticationType.toString());
    RAMConfManager ramConfManager = new RAMConfManager(NutchConfiguration.getUUID(conf), conf);
    nutchServer = new NutchServer(ramConfManager, NutchConfiguration.getUUID(conf));
    nutchServer.start();
  }

  /**
   * Starts the SSL server
   *
   */
  public void startSSLServer() throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException {
    conf.set("restapi.auth", AuthenticationTypeEnum.SSL.toString());
    initializeSSLProperties();
    loadCertificate();
    startServer(AuthenticationTypeEnum.SSL);
  }

  /**
   * Stops the server
   */
  public void stopServer() {
    nutchServer.stop(true);
  }

  protected String getResourcePath(String fileName) throws FileNotFoundException {
    URL resourceFilePath = this.getClass().getClassLoader().getResource(fileName);
    if (resourceFilePath == null) {
      throw new FileNotFoundException("Resource could not found: " + fileName);
    }
    return resourceFilePath.getPath();
  }

  protected void rollbackSystemProperty(String key, String initialValue){
    if (initialValue != null) {
      System.setProperty(key, initialValue);
    } else {
      System.clearProperty(key);
    }
  }

}

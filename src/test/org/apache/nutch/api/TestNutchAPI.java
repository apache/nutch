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

import org.apache.nutch.api.security.AuthenticationTypeEnum;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.restlet.data.ChallengeScheme;

/**
 * Test class for {@link org.apache.nutch.api.NutchServer}
 */
public class TestNutchAPI extends AbstractNutchAPITestBase {

  /**
   * Test insecure connection
   */
  @Test
  public void testInsecure() {
    startServer(AuthenticationTypeEnum.NONE);
    testRequest(200, 8081);
  }

  /**
   * Test Basic Authentication for invalid username/password pair,
   * authorized username/password pair and insufficient privileged username/password pair
   */
  @Test
  public void testBasicAuth() {
    startServer(AuthenticationTypeEnum.BASIC);
    //Check for an invalid username/password pair
    testRequest(401, 8081, "xxx", "xxx", ChallengeScheme.HTTP_BASIC);

    //Check for an authorized username/password pair
    testRequest(200, 8081, "admin", "admin", ChallengeScheme.HTTP_BASIC);

    //Check for an insufficient privileged username/password pair
    testRequest(403, 8081, "user", "user", ChallengeScheme.HTTP_BASIC);
  }

  /**
   * Test Digest Authentication for invalid username/password pair,
   * authorized username/password pair and insufficient privileged username/password pair
   */
  @Test
  public void testDigestAuth() {
    startServer(AuthenticationTypeEnum.DIGEST);
    //Check for an invalid username/password pair
    testRequest(401, 8081, "xxx", "xxx", ChallengeScheme.HTTP_DIGEST);

    //Check for an authorized username/password pair
    testRequest(200, 8081, "admin", "admin", ChallengeScheme.HTTP_DIGEST);

    //Check for an insufficient privileged username/password pair
    testRequest(403, 8081, "user", "user", ChallengeScheme.HTTP_DIGEST);
  }

  /**
   * Test SSL for invalid username/password pair,
   * authorized username/password pair and insufficient privileged username/password pair
   */
  @Ignore
  @Test
  public void testSSL() {
    startServer(AuthenticationTypeEnum.SSL);
    //Check for an invalid username/password pair
    testRequest(401, 8081, "xxx", "xxx");

    //Check for an authorized username/password pair
    testRequest(200, 8081, "admin", "admin");

    //Check for an insufficient privileged username/password pair
    testRequest(403, 8081, "user", "user");
  }

  /**
   * Stops the {@link NutchServer}
   */
  @After
  public  void tearDown() {
    stopServer();
  }

}


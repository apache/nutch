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
package org.apache.nutch.protocol.httpclient;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.codec.binary.Base64;

import org.apache.nutch.protocol.AbstractHttpProtocolPluginTest;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test cases for protocol-httpclient. See also
 * src/test/conf/httpclient-auth-test.xml
 */
public class TestProtocolHttpClient extends AbstractHttpProtocolPluginTest {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  @Override
  protected String getPluginClassName() {
    return "org.apache.nutch.protocol.httpclient.Http";
  }

  /**
   * Tests whether the client can remember cookies.
   * 
   * @throws Exception
   *           If an error occurs or the test case fails.
   */
  @Test
  public void testCookies() throws Exception {
    int port = 47500;
    String responseSetCookies = responseHeader //
        + "Set-Cookie: var1=val1\r\n" //
        + "Set-Cookie: var2=val2\r\n" //
        + "Content-Type: text/html\r\n\r\n" //
        + "<html>\n" //
        + "<head><title>Cookies Set</title></head>" //
        + "<body><p>Cookies have been set.</p></body>" //
        + "</html>";
    String response = responseHeader //
        + "Content-Type: text/html\r\n\r\n" //
        + "<html>\n" //
        + "<head><title>Cookies Found</title></head>" //
        + "<body><p>Cookies found!</p></body>" //
        + "</html>";
    Map<String, byte[]> responses = new TreeMap<>();
    responses.put("/cookies.jsp",
        responseSetCookies.getBytes(StandardCharsets.UTF_8));
    responses.put("/cookies.jsp?cookies=yes",
        response.getBytes(StandardCharsets.UTF_8));
    launchServer(port, (String requestPath) -> {
      return responses.get(requestPath);
    }, (List<String> requestLines) -> {
      // verify whether cookies are set by httpclient
      if (requestLines.get(0).contains("?cookies=yes")) {
        return requestLines.stream().anyMatch((String line) -> {
          return line.startsWith("Cookie:") && line.contains("var1=val1")
              && line.contains("var2=val2");
        });
      }
      return true;
    });
    fetchPage(port, "/cookies.jsp", 200, "text/html");
    fetchPage(port, "/cookies.jsp?cookies=yes", 200, "text/html");
  }

  /**
   * Tests that no pre-emptive authorization headers are sent by the client.
   * 
   * @throws Exception
   *           If an error occurs or the test case fails.
   */
  @Test
  public void testNoPreemptiveAuth() throws Exception {
    int port = 47500;
    String response = responseHeader //
        + "Content-Type: text/html\r\n\r\n" //
        + "<html>\n" //
        + "<head><title>No authorization headers found</title></head>" //
        + "<body>" //
        + "<p>No authorization headers found.</p>" //
        + "</body>" //
        + "</html>";
    launchServer(port, (String requestPath) -> {
      return response.getBytes(UTF_8);
    }, (List<String> requestLines) -> {
      // verify that no "Authentication" header is sent
      return requestLines.stream().noneMatch((String line) -> {
        if (line.startsWith("Authorization:")) {
          LOG.error("Found `Authorization` header, none expected!");
          return true;
        }
        LOG.debug("Verified header: {}", line);
        return false;
      });
    });
    fetchPage(port, "/noauth.jsp", 200, "text/html");
  }

  // see old basic.jsp, digest.jsp, ntlm.jsp
  private static byte[] authenticationResponder(String requestPath, String[] requestHeaders) {

    String authenticationType = "BASIC";
    if (requestPath.startsWith("/digest.jsp")) {
      authenticationType = "DIGEST";
    } else if (requestPath.startsWith("/ntlm.jsp")) {
      authenticationType = "NTLM";
    }

    char id = 'x';
    if (requestPath.endsWith("?case=1")) {
      id = '1';
    } else if (requestPath.endsWith("?case=2")) {
      id = '2';
    }

    String authHeader = getHeader(requestHeaders, "Authorization");
    boolean authenticated = false;
    String authReq = "Basic realm=\"realm" + id + "\"";
    if (authHeader != null) {
      if (authHeader.toUpperCase().startsWith("BASIC")) {
        authenticationType = "BASIC";
        String creds[] = new String(Base64.decodeBase64(authHeader.substring(6)), UTF_8).split(":", 2);
        if (creds[0].equals("user" + id) && creds[1].equals("pass" + id)) {
          authenticated = true;
        }

      } else if (authHeader.toUpperCase().startsWith("DIGEST")) {
        authenticationType = "DIGEST";
        Map<String, String> map = new HashMap<>();
        StringTokenizer tokenizer = new StringTokenizer(
            authHeader.substring(7).trim(), ",");
        while (tokenizer.hasMoreTokens()) {
          String[] param = tokenizer.nextToken().trim().split("=", 2);
          if (param[1].charAt(0) == '"') {
            param[1] = param[1].substring(1, param[1].length() - 1);
          }
          map.put(param[0], param[1]);
        }
        String username = "user" + id;
        if (username.equals(map.get("username"))) {
          authenticated = true;
        }

      } else if (authHeader.toUpperCase().startsWith("NTLM")) {
        authenticationType = "NTLM";
        String username = null;
        String domain = null;
        String host = null;
        byte[] msg = Base64.decodeBase64(authHeader.substring(5));
        if (msg[8] == 1) {
          byte[] type2msg = {
              'N', 'T', 'L', 'M', 'S', 'S', 'P', 0, // NTLMSSP Signature
              2, 0, 0, 0,                           // Type 2 Indicator
              10, 0, 10, 0, 32, 0, 0, 0,            // length, offset
              0x00, 0x02, (byte) 0x81, 0,           // Flags
              1, 2, 3, 4, 5, 6, 7, 8,               // Challenge
              'N', 'U', 'T', 'C', 'H' // NUTCH (Domain)
          };
          // request authentication
          authReq = "NTLM " + Base64.encodeBase64String(type2msg);
        } else if (msg[8] == 3) {
          int length;
          int offset;

          // Get domain name
          length = msg[30] + msg[31] * 256;
          offset = msg[32] + msg[33] * 256;
          domain = new String(msg, offset, length);

          // Get user name
          length = msg[38] + msg[39] * 256;
          offset = msg[40] + msg[41] * 256;
          username = new String(msg, offset, length);

          // Get password
          length = msg[46] + msg[47] * 256;
          offset = msg[48] + msg[49] * 256;
          host = new String(msg, offset, length);

          if ("ntlm_user".equalsIgnoreCase(username)
              && "NUTCH".equalsIgnoreCase(domain)) {
            authenticated = true;
          }
        }
      }
    }

    if (!authenticated) {
      LOG.info("Requesting authentication for realm{} and type {}", id,
          authenticationType);

      if ("DIGEST".equals(authenticationType)) {
        String qop = "qop=\"auth,auth-int\"";
        String nonce = "nonce=\"dcd98b7102dd2f0e8b11d0f600bfb0c093\"";
        String opaque = "opaque=\"5ccc069c403ebaf9f0171e9517f40e41\"";
        authReq = "Digest realm=\"realm" + id + "\", " + qop + ", " + nonce
            + ", " + opaque;

      } else if ("NTLM".equals(authenticationType)) {
        if (!authReq.startsWith("NTLM")) {
          authReq = "NTLM";
        }
      }
      
      String requestAuthorization = "HTTP/1.1 401 Unauthorized\r\n" //
          + "WWW-Authenticate: " + authReq + "\r\n" //
          + "\r\n";
      return requestAuthorization.getBytes(UTF_8);
    }

    LOG.info("User user{} (realm{}, auth. type {}) successfully authenticated",
        id, id, authenticationType);
    String responseAuthenticated = responseHeader //
        + "Content-Type: text/html\r\n\r\n" //
        + "<html>" //
        + "<head><title>" + authenticationType //
        + " Authentication Test</title></head>" //
        + "<body>" //
        + "<p>Hi user" + id + ", you have been successfully authenticated.</p>" //
        + "</body>" //
        + "</html>";
    return responseAuthenticated.getBytes(UTF_8);    
  }
  
  /**
   * Tests default credentials.
   * 
   * @throws Exception
   *           If an error occurs or the test case fails.
   */
  @Test
  public void testDefaultCredentials() throws Exception {
    // the behavior when connecting to port 47502
    // is not configured in httpclient-auth-test.xml
    // which means that authentication is requested first
    int port = 47502;
    launchServer(port, TestProtocolHttpClient::authenticationResponder, null);
    fetchPage(port, "/basic.jsp", 200, "text/html");
  }

  /**
   * Tests basic authentication scheme for various realms.
   * 
   * @throws Exception
   *           If an error occurs or the test case fails.
   */
  @Test
  public void testBasicAuth() throws Exception {
    int port = 47500;
    launchServer(port, TestProtocolHttpClient::authenticationResponder, null);
    fetchPage(port, "/basic.jsp", 200, "text/html");
    fetchPage(port, "/basic.jsp?case=1", 200, "text/html");
    fetchPage(port, "/basic.jsp?case=2", 200, "text/html");
  }

  /**
   * Tests that authentication happens for a defined realm and not for other
   * realms for a host:port when an extra <code>authscope</code> tag is not
   * defined to match all other realms.
   * 
   * @throws Exception
   *           If an error occurs or the test case fails.
   */
  @Test
  public void testOtherRealmsNoAuth() throws Exception {
    int port = 47501;
    launchServer(port, TestProtocolHttpClient::authenticationResponder, null);
    fetchPage(port, "/basic.jsp", 200, "text/html");
    fetchPage(port, "/basic.jsp?case=1", 401, "text/html");
    fetchPage(port, "/basic.jsp?case=2", 401, "text/html");
  }

  /**
   * Tests Digest authentication scheme.
   * 
   * @throws Exception
   *           If an error occurs or the test case fails.
   */
  @Test
  public void testDigestAuth() throws Exception {
    int port = 47500;
    launchServer(port, TestProtocolHttpClient::authenticationResponder, null);
    fetchPage(port, "/digest.jsp", 200, "text/html");
  }

  /**
   * Tests NTLM authentication scheme.
   * 
   * @throws Exception
   *           If an error occurs or the test case fails.
   */
  @Test
  public void testNtlmAuth() throws Exception {
    int port = 47501;
    launchServer(port, TestProtocolHttpClient::authenticationResponder, null);
    fetchPage(port, "/ntlm.jsp", 200, "text/html");
  }

}

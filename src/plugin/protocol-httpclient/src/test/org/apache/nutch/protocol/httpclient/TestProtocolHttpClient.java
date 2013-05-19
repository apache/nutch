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

import java.net.MalformedURLException;
import java.net.URL;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.storage.WebPage;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

/**
 * Test cases for protocol-httpclient.
 * 
 * @author Susam Pal
 */
public class TestProtocolHttpClient {

	private Server server;
	private Configuration conf;
	private static final String RES_DIR = System.getProperty("test.data", ".");
	private int port;
	private Http http = new Http();

  @Before
	public void setUp() throws Exception {

		server = new Server();
		
//		Context scontext = new Context();
//		scontext.setContextPath("/");
//		scontext.setResourceBase(RES_DIR);
//		// servlet handler?
//		scontext.addServlet("JSP", "*.jsp",
//				"org.apache.jasper.servlet.JspServlet");
//		scontext.addHandler(new ResourceHandler());

		Context root = new Context(server,"/",Context.SESSIONS);
		root.setContextPath("/");
		root.setResourceBase(RES_DIR);
		ServletHolder sh = new ServletHolder(org.apache.jasper.servlet.JspServlet.class);
		root.addServlet(sh, "*.jsp");

		conf = new Configuration();
		conf.addResource("nutch-default.xml");
		conf.addResource("nutch-site-test.xml");

		http = new Http();
		http.setConf(conf);
	}

  @After
	public void tearDown() throws Exception {
		server.stop();
	}

	/**
	 * Tests whether the client can remember cookies.
	 * 
	 * @throws Exception
	 *             If an error occurs or the test case fails.
	 */
	@Test
	public void testCookies() throws Exception {
		startServer(47500);
		fetchPage("/cookies.jsp", 200);
		fetchPage("/cookies.jsp?cookie=yes", 200);
		tearDown();
	}

	/**
	 * Tests that no pre-emptive authorization headers are sent by the client.
	 * 
	 * @throws Exception
	 *             If an error occurs or the test case fails.
	 */
	@Test
	public void testNoPreemptiveAuth() throws Exception {
		startServer(47500);
		fetchPage("/noauth.jsp", 200);
		tearDown();
	}

	/**
	 * Tests default credentials.
	 * 
	 * @throws Exception
	 *             If an error occurs or the test case fails.
	 */
	@Test
	public void testDefaultCredentials() throws Exception {
		startServer(47502);
		fetchPage("/basic.jsp", 200);
		tearDown();
	}

	/**
	 * Tests basic authentication scheme for various realms.
	 * 
	 * @throws Exception
	 *             If an error occurs or the test case fails.
	 */
	@Test
	public void testBasicAuth() throws Exception {
		startServer(47500);
		fetchPage("/basic.jsp", 200);
		fetchPage("/basic.jsp?case=1", 200);
		fetchPage("/basic.jsp?case=2", 200);
		tearDown();
	}

	/**
	 * Tests that authentication happens for a defined realm and not for other
	 * realms for a host:port when an extra <code>authscope</code> tag is not
	 * defined to match all other realms.
	 * 
	 * @throws Exception
	 *             If an error occurs or the test case fails.
	 */
	@Test
	public void testOtherRealmsNoAuth() throws Exception {
		startServer(47501);
		fetchPage("/basic.jsp", 200);
		fetchPage("/basic.jsp?case=1", 401);
		fetchPage("/basic.jsp?case=2", 401);
		tearDown();
	}

	/**
	 * Tests Digest authentication scheme.
	 * 
	 * @throws Exception
	 *             If an error occurs or the test case fails.
	 */
	@Test
	public void testDigestAuth() throws Exception {
		startServer(47500);
		fetchPage("/digest.jsp", 200);
		tearDown();
	}

	/**
	 * Tests NTLM authentication scheme.
	 * 
	 * @throws Exception
	 *             If an error occurs or the test case fails.
	 */
	@Test
	public void testNtlmAuth() throws Exception {
		startServer(47501);
		fetchPage("/ntlm.jsp", 200);
		tearDown();
	}

	/**
	 * Starts the Jetty server at a specified port.
	 * 
	 * @param portno
	 *            Port number.
	 * @throws Exception
	 *             When an error occurs.
	 */
	private void startServer(int portno) throws Exception {
		port = portno;

		SelectChannelConnector connector1 = new SelectChannelConnector();
		connector1.setHost("127.0.0.1");
		connector1.setPort(port);

		server.addConnector(connector1);
		server.start();
	}

	/**
	 * Fetches the specified <code>page</code> from the local Jetty server and
	 * checks whether the HTTP response status code matches with the expected
	 * code.
	 * 
	 * @param page
	 *            Page to be fetched.
	 * @param expectedCode
	 *            HTTP response status code expected while fetching the page.
	 * @throws Exception
	 *             When an error occurs or test case fails.
	 */
	private void fetchPage(String page, int expectedCode) throws Exception {
		URL url = new URL("http", "127.0.0.1", port, page);
		Response response = null;
		response = http.getResponse(url, new WebPage(), true);

		int code = response.getCode();
		assertEquals("HTTP Status Code for " + url, expectedCode, code);
	}

	/**
	 * Returns an URL to the specified page.
	 * 
	 * @param page
	 *            Page available in the local Jetty server.
	 * @throws MalformedURLException
	 *             If an URL can not be formed.
	 */
	private URL getURL(String page) throws MalformedURLException {
		return new URL("http", "127.0.0.1", port, page);
	}
}

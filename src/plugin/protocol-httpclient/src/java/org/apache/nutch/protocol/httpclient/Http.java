/**
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

// JDK imports
import java.io.InputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// HTTP Client imports
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.NTCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.commons.httpclient.protocol.SSLProtocolSocketFactory;

// Nutch imports
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

/**
 * This class is a protocol plugin that configures an HTTP client for Basic,
 * Digest and NTLM authentication schemes for web server as well as proxy
 * server. It takes care of HTTPS protocol as well as cookies in a single fetch
 * session.
 * 
 * @author Susam Pal
 */
public class Http extends HttpBase {

	public static final Logger LOG = LoggerFactory.getLogger(Http.class);

	private static MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();

	// Since the Configuration has not yet been set,
	// then an unconfigured client is returned.
	private static HttpClient client = new HttpClient(connectionManager);
	private static String defaultUsername;
	private static String defaultPassword;
	private static String defaultRealm;
	private static String defaultScheme;
	private static String authFile;
	private static String agentHost;
	private static boolean authRulesRead = false;
	private static Configuration conf;

	int maxThreadsTotal = 10;

	private String proxyUsername;
	private String proxyPassword;
	private String proxyRealm;

	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	static {
		FIELDS.add(WebPage.Field.MODIFIED_TIME);
		FIELDS.add(WebPage.Field.HEADERS);
	}

	@Override
	public Collection<Field> getFields() {
		return FIELDS;
	}
	
	/**
	 * Returns the configured HTTP client.
	 * 
	 * @return HTTP client
	 */
	static synchronized HttpClient getClient() {
		return client;
	}

	/**
	 * Constructs this plugin.
	 */
	public Http() {
		super(LOG);
	}

	/**
	 * Reads the configuration from the Nutch configuration files and sets the
	 * configuration.
	 * 
	 * @param conf
	 *            Configuration
	 */
	public void setConf(Configuration conf) {
		super.setConf(conf);
		Http.conf = conf;
		this.maxThreadsTotal = conf.getInt("fetcher.threads.fetch", 10);
		this.proxyUsername = conf.get("http.proxy.username", "");
		this.proxyPassword = conf.get("http.proxy.password", "");
		this.proxyRealm = conf.get("http.proxy.realm", "");
		agentHost = conf.get("http.agent.host", "");
		authFile = conf.get("http.auth.file", "");
		configureClient();
		try {
			setCredentials();
		} catch (Exception ex) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Could not read " + authFile + " : "
						+ ex.getMessage());
			}
		}
	}

	/**
	 * Main method.
	 * 
	 * @param args
	 *            Command line arguments
	 */
	public static void main(String[] args) throws Exception {
		Http http = new Http();
		http.setConf(NutchConfiguration.create());
		main(http, args);
	}

	/**
	 * Fetches the <code>url</code> with a configured HTTP client and gets the
	 * response.
	 * 
	 * @param url
	 *            URL to be fetched
	 * @param datum
	 *            Crawl data
	 * @param redirect
	 *            Follow redirects if and only if true
	 * @return HTTP response
	 */
	protected Response getResponse(URL url, WebPage page, boolean redirect)
			throws ProtocolException, IOException {
		resolveCredentials(url);
		return new HttpResponse(this, url, page, redirect);
	}

	/**
	 * Configures the HTTP client
	 */
	private void configureClient() {

		// Set up an HTTPS socket factory that accepts self-signed certs.
	  ProtocolSocketFactory factory = new SSLProtocolSocketFactory();
		Protocol https = new Protocol("https", factory, 443);
		Protocol.registerProtocol("https", https);

		HttpConnectionManagerParams params = connectionManager.getParams();
		params.setConnectionTimeout(timeout);
		params.setSoTimeout(timeout);
		params.setSendBufferSize(BUFFER_SIZE);
		params.setReceiveBufferSize(BUFFER_SIZE);
		params.setMaxTotalConnections(maxThreadsTotal);

		// executeMethod(HttpMethod) seems to ignore the connection timeout on
		// the connection manager.
		// set it explicitly on the HttpClient.
		client.getParams().setConnectionManagerTimeout(timeout);

		HostConfiguration hostConf = client.getHostConfiguration();
		ArrayList<Header> headers = new ArrayList<Header>();
		// Set the User Agent in the header
		headers.add(new Header("User-Agent", userAgent));
		// prefer English
		headers.add(new Header("Accept-Language",
				"en-us,en-gb,en;q=0.7,*;q=0.3"));
		// prefer UTF-8
		headers.add(new Header("Accept-Charset",
				"utf-8,ISO-8859-1;q=0.7,*;q=0.7"));
		// prefer understandable formats
		headers.add(new Header(
				"Accept",
				"text/html,application/xml;q=0.9,application/xhtml+xml,text/xml;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5"));
		// accept gzipped content
		headers.add(new Header("Accept-Encoding", "x-gzip, gzip, deflate"));
		hostConf.getParams().setParameter("http.default-headers", headers);

		// HTTP proxy server details
		if (useProxy) {
			hostConf.setProxy(proxyHost, proxyPort);

			if (proxyUsername.length() > 0) {

				AuthScope proxyAuthScope = getAuthScope(this.proxyHost,
						this.proxyPort, this.proxyRealm);

				NTCredentials proxyCredentials = new NTCredentials(
						this.proxyUsername, this.proxyPassword, Http.agentHost,
						this.proxyRealm);

				client.getState().setProxyCredentials(proxyAuthScope,
						proxyCredentials);
			}
		}

	}

	/**
	 * Reads authentication configuration file (defined as 'http.auth.file' in
	 * Nutch configuration file) and sets the credentials for the configured
	 * authentication scopes in the HTTP client object.
	 * 
	 * @throws ParserConfigurationException
	 *             If a document builder can not be created.
	 * @throws SAXException
	 *             If any parsing error occurs.
	 * @throws IOException
	 *             If any I/O error occurs.
	 */
	private static synchronized void setCredentials()
			throws ParserConfigurationException, SAXException, IOException {

		if (authRulesRead)
			return;

		authRulesRead = true; // Avoid re-attempting to read

		InputStream is = conf.getConfResourceAsInputStream(authFile);
		if (is != null) {
			Document doc = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder().parse(is);

			Element rootElement = doc.getDocumentElement();
			if (!"auth-configuration".equals(rootElement.getTagName())) {
				if (LOG.isWarnEnabled())
					LOG.warn("Bad auth conf file: root element <"
							+ rootElement.getTagName() + "> found in "
							+ authFile + " - must be <auth-configuration>");
			}

			// For each set of credentials
			NodeList credList = rootElement.getChildNodes();
			for (int i = 0; i < credList.getLength(); i++) {
				Node credNode = credList.item(i);
				if (!(credNode instanceof Element))
					continue;

				Element credElement = (Element) credNode;
				if (!"credentials".equals(credElement.getTagName())) {
					if (LOG.isWarnEnabled())
						LOG.warn("Bad auth conf file: Element <"
								+ credElement.getTagName()
								+ "> not recognized in " + authFile
								+ " - expected <credentials>");
					continue;
				}

				String username = credElement.getAttribute("username");
				String password = credElement.getAttribute("password");

				// For each authentication scope
				NodeList scopeList = credElement.getChildNodes();
				for (int j = 0; j < scopeList.getLength(); j++) {
					Node scopeNode = scopeList.item(j);
					if (!(scopeNode instanceof Element))
						continue;

					Element scopeElement = (Element) scopeNode;

					if ("default".equals(scopeElement.getTagName())) {

						// Determine realm and scheme, if any
						String realm = scopeElement.getAttribute("realm");
						String scheme = scopeElement.getAttribute("scheme");

						// Set default credentials
						defaultUsername = username;
						defaultPassword = password;
						defaultRealm = realm;
						defaultScheme = scheme;

						if (LOG.isTraceEnabled()) {
							LOG.trace("Credentials - username: " + username
									+ "; set as default" + " for realm: "
									+ realm + "; scheme: " + scheme);
						}

					} else if ("authscope".equals(scopeElement.getTagName())) {

						// Determine authentication scope details
						String host = scopeElement.getAttribute("host");
						int port = -1; // For setting port to AuthScope.ANY_PORT
						try {
							port = Integer.parseInt(scopeElement
									.getAttribute("port"));
						} catch (Exception ex) {
							// do nothing, port is already set to any port
						}
						String realm = scopeElement.getAttribute("realm");
						String scheme = scopeElement.getAttribute("scheme");

						// Set credentials for the determined scope
						AuthScope authScope = getAuthScope(host, port, realm,
								scheme);
						NTCredentials credentials = new NTCredentials(username,
								password, agentHost, realm);

						client.getState()
								.setCredentials(authScope, credentials);

						if (LOG.isTraceEnabled()) {
							LOG.trace("Credentials - username: " + username
									+ "; set for AuthScope - " + "host: "
									+ host + "; port: " + port + "; realm: "
									+ realm + "; scheme: " + scheme);
						}

					} else {
						if (LOG.isWarnEnabled())
							LOG.warn("Bad auth conf file: Element <"
									+ scopeElement.getTagName()
									+ "> not recognized in " + authFile
									+ " - expected <authscope>");
					}
				}
				is.close();
			}
		}
	}

	/**
	 * If credentials for the authentication scope determined from the specified
	 * <code>url</code> is not already set in the HTTP client, then this method
	 * sets the default credentials to fetch the specified <code>url</code>. If
	 * credentials are found for the authentication scope, the method returns
	 * without altering the client.
	 * 
	 * @param url
	 *            URL to be fetched
	 */
	private void resolveCredentials(URL url) {

		if (defaultUsername != null && defaultUsername.length() > 0) {

			int port = url.getPort();
			if (port == -1) {
				if ("https".equals(url.getProtocol()))
					port = 443;
				else
					port = 80;
			}

			AuthScope scope = new AuthScope(url.getHost(), port);

			if (client.getState().getCredentials(scope) != null) {
				if (LOG.isTraceEnabled())
					LOG.trace("Pre-configured credentials with scope - host: "
							+ url.getHost() + "; port: " + port
							+ "; found for url: " + url);

				// Credentials are already configured, so do nothing and return
				return;
			}

			if (LOG.isTraceEnabled())
				LOG.trace("Pre-configured credentials with scope -  host: "
						+ url.getHost() + "; port: " + port
						+ "; not found for url: " + url);

			AuthScope serverAuthScope = getAuthScope(url.getHost(), port,
					defaultRealm, defaultScheme);

			NTCredentials serverCredentials = new NTCredentials(
					defaultUsername, defaultPassword, agentHost, defaultRealm);

			client.getState()
					.setCredentials(serverAuthScope, serverCredentials);
		}
	}

	/**
	 * Returns an authentication scope for the specified <code>host</code>,
	 * <code>port</code>, <code>realm</code> and <code>scheme</code>.
	 * 
	 * @param host
	 *            Host name or address.
	 * @param port
	 *            Port number.
	 * @param realm
	 *            Authentication realm.
	 * @param scheme
	 *            Authentication scheme.
	 */
	private static AuthScope getAuthScope(String host, int port, String realm,
			String scheme) {

		if (host.length() == 0)
			host = null;

		if (port < 0)
			port = -1;

		if (realm.length() == 0)
			realm = null;

		if (scheme.length() == 0)
			scheme = null;

		return new AuthScope(host, port, realm, scheme);
	}

	/**
	 * Returns an authentication scope for the specified <code>host</code>,
	 * <code>port</code> and <code>realm</code>.
	 * 
	 * @param host
	 *            Host name or address.
	 * @param port
	 *            Port number.
	 * @param realm
	 *            Authentication realm.
	 */
	private static AuthScope getAuthScope(String host, int port, String realm) {

		return getAuthScope(host, port, realm, "");
	}

}

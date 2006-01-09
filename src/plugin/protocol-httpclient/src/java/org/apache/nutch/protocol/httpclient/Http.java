/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

// HTTP Client imports
import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.NTCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.protocol.Protocol;

// Nutch imports
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.util.NutchConf;


public class Http extends HttpBase {

  public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.net.Http");

  static {
    if (NutchConf.get().getBoolean("http.verbose", false)) {
      LOG.setLevel(Level.FINE);
    } else {                                      // shush about redirects
      Logger.getLogger("org.apache.commons.httpclient.HttpMethodDirector")
        .setLevel(Level.WARNING);
    }
  }

  private static MultiThreadedHttpConnectionManager connectionManager =
          new MultiThreadedHttpConnectionManager();
  
  private static HttpClient client;

  static synchronized HttpClient getClient() {
    if (client != null) return client;
    configureClient();
    return client;
  }

  static int MAX_THREADS_TOTAL = NutchConf.get().getInt("fetcher.threads.fetch", 10);
  static String NTLM_USERNAME = NutchConf.get().get("http.auth.ntlm.username", "");
  static String NTLM_PASSWORD = NutchConf.get().get("http.auth.ntlm.password", "");
  static String NTLM_DOMAIN = NutchConf.get().get("http.auth.ntlm.domain", "");
  static String NTLM_HOST = NutchConf.get().get("http.auth.ntlm.host", "");

  static {
    LOG.info("http.auth.ntlm.username = " + NTLM_USERNAME);
  }


  public Http() {
    super(LOG);
  }

  public static void main(String[] args) throws Exception {
    main(new Http(), args);
  }

  protected Response getResponse(URL url, CrawlDatum datum, boolean redirect)
    throws ProtocolException, IOException {
    return new HttpResponse(url, datum, redirect);
  }
  
  private static void configureClient() {

    // get a client isntance -- we just need one.

    client = new HttpClient(connectionManager);

    // Set up an HTTPS socket factory that accepts self-signed certs.
    Protocol dummyhttps = new Protocol("https", new DummySSLProtocolSocketFactory(), 443);
    Protocol.registerProtocol("https", dummyhttps);
    
    HttpConnectionManagerParams params = connectionManager.getParams();
    params.setConnectionTimeout(TIMEOUT);
    params.setSoTimeout(TIMEOUT);
    params.setSendBufferSize(BUFFER_SIZE);
    params.setReceiveBufferSize(BUFFER_SIZE);
    params.setMaxTotalConnections(MAX_THREADS_TOTAL);
    if (MAX_THREADS_TOTAL > MAX_THREADS_PER_HOST) {
      params.setDefaultMaxConnectionsPerHost(MAX_THREADS_PER_HOST);
    } else {
      params.setDefaultMaxConnectionsPerHost(MAX_THREADS_TOTAL);
    }

    HostConfiguration hostConf = client.getHostConfiguration();
    ArrayList headers = new ArrayList();
    // prefer English
    headers.add(new Header("Accept-Language", "en-us,en-gb,en;q=0.7,*;q=0.3"));
    // prefer UTF-8
    headers.add(new Header("Accept-Charset", "utf-8,ISO-8859-1;q=0.7,*;q=0.7"));
    // prefer understandable formats
    headers.add(new Header("Accept",
            "text/html,application/xml;q=0.9,application/xhtml+xml,text/xml;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5"));
    hostConf.getParams().setParameter("http.default-headers", headers);
    if (PROXY) {
      hostConf.setProxy(PROXY_HOST, PROXY_PORT);
    }
    if (NTLM_USERNAME.length() > 0) {
      Credentials ntCreds = new NTCredentials(NTLM_USERNAME, NTLM_PASSWORD, NTLM_HOST, NTLM_DOMAIN);
      client.getState().setCredentials(new AuthScope(NTLM_HOST, AuthScope.ANY_PORT), ntCreds);

      LOG.info("Added NTLM credentials for " + NTLM_USERNAME);
    }
    LOG.info("Configured Client");
  }
}
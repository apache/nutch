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
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;


public class Http extends HttpBase {

  public static final Log LOG = LogFactory.getLog(Http.class);

  private static MultiThreadedHttpConnectionManager connectionManager =
          new MultiThreadedHttpConnectionManager();

  // Since the Configuration has not yet been set,
  // then an unconfigured client is returned.
  private static HttpClient client = new HttpClient(connectionManager);

  static synchronized HttpClient getClient() {
    return client;
  }

  boolean verbose = false;
  int maxThreadsTotal = 10;
  String ntlmUsername = "";
  String ntlmPassword = "";
  String ntlmDomain = "";
  String ntlmHost = "";

  public Http() {
    super(LOG);
  }

  public void setConf(Configuration conf) {
    super.setConf(conf);
    this.maxThreadsTotal = conf.getInt("fetcher.threads.fetch", 10);
    this.ntlmUsername = conf.get("http.auth.ntlm.username", "");
    this.ntlmPassword = conf.get("http.auth.ntlm.password", "");
    this.ntlmDomain = conf.get("http.auth.ntlm.domain", "");
    this.ntlmHost = conf.get("http.auth.ntlm.host", "");
    configureClient();
  }

  public static void main(String[] args) throws Exception {
    Http http = new Http();
    http.setConf(NutchConfiguration.create());
    main(http, args);
  }

  protected Response getResponse(URL url, CrawlDatum datum, boolean redirect)
    throws ProtocolException, IOException {
    return new HttpResponse(this, url, datum, redirect);
  }
  
  private void configureClient() {

    // Set up an HTTPS socket factory that accepts self-signed certs.
    Protocol dummyhttps = new Protocol("https", new DummySSLProtocolSocketFactory(), 443);
    Protocol.registerProtocol("https", dummyhttps);
    
    HttpConnectionManagerParams params = connectionManager.getParams();
    params.setConnectionTimeout(timeout);
    params.setSoTimeout(timeout);
    params.setSendBufferSize(BUFFER_SIZE);
    params.setReceiveBufferSize(BUFFER_SIZE);
    params.setMaxTotalConnections(maxThreadsTotal);
    if (maxThreadsTotal > maxThreadsPerHost) {
      params.setDefaultMaxConnectionsPerHost(maxThreadsPerHost);
    } else {
      params.setDefaultMaxConnectionsPerHost(maxThreadsTotal);
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
    // accept gzipped content
    headers.add(new Header("Accept-Encoding", "x-gzip, gzip"));
    hostConf.getParams().setParameter("http.default-headers", headers);
    if (useProxy) {
      hostConf.setProxy(proxyHost, proxyPort);
    }
    if (ntlmUsername.length() > 0) {
      Credentials ntCreds = new NTCredentials(ntlmUsername, ntlmPassword, ntlmHost, ntlmDomain);
      client.getState().setCredentials(new AuthScope(ntlmHost, AuthScope.ANY_PORT), ntCreds);

      if (LOG.isInfoEnabled()) {
        LOG.info("Added NTLM credentials for " + ntlmUsername);
      }
    }
    if (LOG.isInfoEnabled()) { LOG.info("Configured Client"); }
  }
}

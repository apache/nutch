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
package org.apache.nutch.indexwriter.solr;

import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.client.solrj.SolrServer;

import java.net.MalformedURLException;

public class SolrUtils {

  public static Logger LOG = LoggerFactory.getLogger(SolrUtils.class);

  private static SolrServer server;

  public static SolrServer getSolrServer(JobConf job)
      throws MalformedURLException {

    boolean auth = job.getBoolean(SolrConstants.USE_AUTH, false);

    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    // Check for username/password
    if (auth) {
      String username = job.get(SolrConstants.USERNAME);
      LOG.info("Authenticating as: " + username);
      AuthScope scope = new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT,
          AuthScope.ANY_REALM, AuthScope.ANY_SCHEME);
      credentialsProvider.setCredentials(scope, 
          new UsernamePasswordCredentials(username, job.get(SolrConstants.PASSWORD)));
    }
    CloseableHttpClient client = 
        HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build();

    String solrServer = job.get(SolrConstants.SERVER_TYPE, "http");
    String zkHost = job.get(SolrConstants.ZOOKEEPER_URL, null);
    String solrServerUrl = job.get(SolrConstants.SERVER_URL);

    switch (solrServer) {
    case "cloud":
      server = new CloudSolrServer(zkHost);
      LOG.debug("CloudSolrServer selected as indexing server.");
      break;
    case "concurrent":
      server = new ConcurrentUpdateSolrServer(solrServerUrl, client, 1000, 10);
      LOG.debug("ConcurrentUpdateSolrServer selected as indexing server.");
      break;
    case "http":
      if (auth) {
        server = new HttpSolrServer(solrServerUrl, client);
      } else {
        server = new HttpSolrServer(solrServerUrl);
      }
      LOG.debug("HttpSolrServer selected as indexing server.");
      break;
    case "lb":
      String[] lbServerString = job.get(SolrConstants.LOADBALANCE_URLS).split(",");
      server = new LBHttpSolrServer(client, lbServerString);
      LOG.debug("LBHttpSolrServer selected as indexing server.");
      break;
    default:
      if (auth) {
        server = new HttpSolrServer(solrServerUrl, client);
      } else {
        server = new HttpSolrServer(solrServerUrl);
      }
      LOG.debug("HttpSolrServer selected as indexing server.");
      break;
    }
    return server;
  }

  public static String stripNonCharCodepoints(String input) {
    StringBuilder retval = new StringBuilder();
    char ch;

    for (int i = 0; i < input.length(); i++) {
      ch = input.charAt(i);

      // Strip all non-characters
      // http://unicode.org/cldr/utility/list-unicodeset.jsp?a=[:Noncharacter_Code_Point=True:]
      // and non-printable control characters except tabulator, new line and
      // carriage return
      if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {

        retval.append(ch);
      }
    }

    return retval.toString();
  }
}
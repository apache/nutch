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

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import java.util.Arrays;
import java.util.List;

public class SolrUtils {

  static CloudSolrClient getCloudSolrClient(List<String> urls) {
    CloudSolrClient sc = new CloudSolrClient.Builder(urls)
        .withParallelUpdates(true).build();
    sc.connect();
    return sc;
  }

  static CloudSolrClient getCloudSolrClient(List<String> urls, String username,
      String password) {
    // Building HTTP client
    CredentialsProvider provider = new BasicCredentialsProvider();
    UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(
        username, password);
    provider.setCredentials(AuthScope.ANY, credentials);

    HttpClient client = HttpClientBuilder.create()
        .setDefaultCredentialsProvider(provider).build();

    // Building the client
    CloudSolrClient sc = new CloudSolrClient.Builder(urls)
        .withParallelUpdates(true).withHttpClient(client).build();
    sc.connect();
    return sc;
  }

  /**
   * Creates a new SolrClient, passing an Authorization header on the requests'
   * HTTP Header:
   * 
   * <pre>
   * Authorization: headerName headerValue
   * </pre>
   * 
   * ie.
   * 
   * <pre>
   * Authorization: Bearer XXXXXXXXXXX
   * </pre>
   * 
   * @param url
   *          Sorl URL
   * @param headerName
   *          Header name send on the Authorization: Bearer, Token, etc.
   * @param headerValue
   *          Header value send on the Authorization: JWT_TOKEN
   * @return CloudSolrClient
   */
  static CloudSolrClient getCloudSolrClientHeaderAuthorization(
      List<String> urls, String headerName, String headerValue) {
    // Building http client
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    httpClientBuilder.setDefaultHeaders(Arrays.asList(
        new BasicHeader("Authorization", headerName + " " + headerValue)));
    // Building the client
    CloudSolrClient sc = new CloudSolrClient.Builder(urls)
        .withParallelUpdates(true).withHttpClient(httpClientBuilder.build())
        .build();
    sc.connect();
    return sc;
  }

  static SolrClient getHttpSolrClient(String url) {
    return new HttpSolrClient.Builder(url).build();
  }

  /**
   * Creates a new SolrClient, passing an Authorization header on the requests'
   * HTTP Header:
   * 
   * <pre>
   * Authorization: headerName headerValue
   * </pre>
   * 
   * ie.
   * 
   * <pre>
   * Authorization: Bearer XXXXXXXXXXX
   * </pre>
   * 
   * @param url
   *          Solr URL
   * @param headerName
   *          Header name send on the Authorization: Bearer, Token, etc.
   * @param headerValue
   *          Header value send on the Authorization: JWT_TOKEN
   * @return SolrClient
   */
  static SolrClient getHttpSolrClientHeaderAuthorization(String url,
      String headerName, String headerValue) {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    httpClientBuilder.setDefaultHeaders(Arrays.asList(
        new BasicHeader("Authorization", headerName + " " + headerValue)));
    return new HttpSolrClient.Builder(url)
        .withHttpClient(httpClientBuilder.build()).build();
  }

  /**
   * Creates a new SolrClient, using Basic Authentication.
   * 
   * @param url
   *          Solr URL
   * @param username
   *          Username
   * @param password
   *          Password
   * @return SolrClient
   */
  static SolrClient getHttpSolrClient(String url, String username,
      String password) {
    CredentialsProvider provider = new BasicCredentialsProvider();
    UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(
        username, password);
    provider.setCredentials(AuthScope.ANY, credentials);
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    httpClientBuilder.setDefaultCredentialsProvider(provider);
    return new HttpSolrClient.Builder(url)
        .withHttpClient(httpClientBuilder.build()).build();
  }

  static String stripNonCharCodepoints(String input) {
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

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
package org.apache.nutch.net.protocols;

import java.net.URL;

import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Metadata;

/**
 * A response interface. Makes all protocols model HTTP.
 */
public interface Response extends HttpHeaders {

  /** Key to hold the HTTP request if <code>store.http.request</code> is true */
  public static final String REQUEST = "_request_";

  /**
   * Key to hold the HTTP response header if <code>store.http.headers</code> is
   * true
   */
  public static final String RESPONSE_HEADERS = "_response.headers_";

  /**
   * Key to hold the IP address the request is sent to if
   * <code>store.ip.address</code> is true
   */
  public static final String IP_ADDRESS = "_ip_";

  /**
   * Key to hold the HTTP and SSL/TLS protocol versions if
   * <code>store.protocol.versions</code> is true.
   */
  public static final String PROTOCOL_VERSIONS = "_protocol_versions_";

  /**
   * Key to hold the SSL/TLS cipher suites
   * <code>store.protocol.versions</code> is true.
   */
  public static final String CIPHER_SUITES = "_cipher_suites_";

  /**
   * Key to hold the time when the page has been fetched
   */
  public static final String FETCH_TIME = "nutch.fetch.time";

  /**
   * Key to hold boolean whether content has been truncated, e.g., because it
   * exceeds <code>http.content.limit</code>
   */
  public static final String TRUNCATED_CONTENT = "http.content.truncated";

  /**
   * Key to hold reason why content has been truncated, see
   * {@link TruncatedContentReason}
   */
  public static final String TRUNCATED_CONTENT_REASON = "http.content.truncated.reason";

  public static enum TruncatedContentReason {
    NOT_TRUNCATED,
    /** fetch exceeded configured http.content.limit */
    LENGTH,
    /** fetch exceeded configured http.time.limit */
    TIME,
    /** network disconnect or timeout during fetch */
    DISCONNECT,
    /** implementation internal reason */
    INTERNAL,
    /** unknown reason */
    UNSPECIFIED
  };

  /**
   * Get the URL used to retrieve this response.
   * @return {@link java.net.URL}
   */
  public URL getUrl();

  /**
   * Get the response code.
   * @return protocol response code (int)
   * */
  public int getCode();

  /**
   * Get the value of a named header.
   * @param name key of the header you wish to retrieve
   * @return header value
   */
  public String getHeader(String name);

  /**
   * Get all the headers.
   * @return populated headers {@link org.apache.nutch.metadata.Metadata}
   */
  public Metadata getHeaders();

  /**
   * Get the full content of the response.
   * @return a byte array representing the response content
   */
  public byte[] getContent();

}

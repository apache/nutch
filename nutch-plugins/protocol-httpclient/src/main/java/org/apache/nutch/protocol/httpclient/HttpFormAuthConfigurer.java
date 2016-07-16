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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HttpFormAuthConfigurer {
  private String loginUrl;
  private String loginFormId;
  /**
   * The data posted to login form, such as username(or email), password
   */
  private Map<String, String> loginPostData;
  /**
   * In case we need add additional headers.
   */
  private Map<String, String> additionalPostHeaders;
  /**
   * If http post login returns redirect code: 301 or 302, 
   * Http Client will automatically follow the redirect.
   */
  private boolean loginRedirect;
  /**
   * Used when we need remove some form fields.
   */
  private Set<String> removedFormFields;

  public HttpFormAuthConfigurer() {
  }

  public String getLoginUrl() {
    return loginUrl;
  }

  public HttpFormAuthConfigurer setLoginUrl(String loginUrl) {
    this.loginUrl = loginUrl;
    return this;
  }

  public String getLoginFormId() {
    return loginFormId;
  }

  public HttpFormAuthConfigurer setLoginFormId(String loginForm) {
    this.loginFormId = loginForm;
    return this;
  }

  public Map<String, String> getLoginPostData() {
    return loginPostData == null ? new HashMap<String, String>()
        : loginPostData;
  }

  public HttpFormAuthConfigurer setLoginPostData(
      Map<String, String> loginPostData) {
    this.loginPostData = loginPostData;
    return this;
  }

  public Map<String, String> getAdditionalPostHeaders() {
    return additionalPostHeaders == null ? new HashMap<String, String>()
        : additionalPostHeaders;
  }

  public HttpFormAuthConfigurer setAdditionalPostHeaders(
      Map<String, String> additionalPostHeaders) {
    this.additionalPostHeaders = additionalPostHeaders;
    return this;
  }

  public boolean isLoginRedirect() {
    return loginRedirect;
  }

  public HttpFormAuthConfigurer setLoginRedirect(boolean redirect) {
    this.loginRedirect = redirect;
    return this;
  }

  public Set<String> getRemovedFormFields() {
    return removedFormFields == null ? new HashSet<String>()
        : removedFormFields;
  }

  public HttpFormAuthConfigurer setRemovedFormFields(
      Set<String> removedFormFields) {
    this.removedFormFields = removedFormFields;
    return this; }
}

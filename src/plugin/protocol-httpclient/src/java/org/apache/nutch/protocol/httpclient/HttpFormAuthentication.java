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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpFormAuthentication {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(HttpFormAuthentication.class);
  private static Map<String, String> defaultLoginHeaders = new HashMap<String, String>();

  static {
    defaultLoginHeaders.put("User-Agent", "Mozilla/5.0");
    defaultLoginHeaders.put("Accept",
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
    defaultLoginHeaders.put("Accept-Language", "en-US,en;q=0.5");
    defaultLoginHeaders.put("Connection", "keep-alive");
    defaultLoginHeaders.put("Content-Type",
        "application/x-www-form-urlencoded");
  }

  private HttpClient client;
  private HttpFormAuthConfigurer authConfigurer = new HttpFormAuthConfigurer();
  private String cookies;

  public HttpFormAuthentication(HttpFormAuthConfigurer authConfigurer,
      HttpClient client, Http http) {
    this.authConfigurer = authConfigurer;
    this.client = client;
    defaultLoginHeaders.put("Accept", http.getAccept());
    defaultLoginHeaders.put("Accept-Language", http.getAcceptLanguage());
    defaultLoginHeaders.put("User-Agent", http.getUserAgent());
  }

  public HttpFormAuthentication(String loginUrl, String loginForm,
      Map<String, String> loginPostData,
      Map<String, String> additionalPostHeaders,
      Set<String> removedFormFields) {
    this.authConfigurer.setLoginUrl(loginUrl);
    this.authConfigurer.setLoginFormId(loginForm);
    this.authConfigurer.setLoginPostData(
        loginPostData == null ? new HashMap<String, String>() : loginPostData);
    this.authConfigurer.setAdditionalPostHeaders(additionalPostHeaders == null
        ? new HashMap<String, String>() : additionalPostHeaders);
    this.authConfigurer.setRemovedFormFields(
        removedFormFields == null ? new HashSet<String>() : removedFormFields);
    this.client = new HttpClient();
  }

  public void login() throws Exception {
    // make sure cookies are turned on
    CookieHandler.setDefault(new CookieManager());
    String pageContent = httpGetPageContent(authConfigurer.getLoginUrl());
    List<NameValuePair> params = getLoginFormParams(pageContent);
    sendPost(authConfigurer.getLoginUrl(), params);
  }

  private void sendPost(String url, List<NameValuePair> params)
      throws Exception {
    PostMethod post = null;
    try {
      if (authConfigurer.isLoginRedirect()) {
        post = new PostMethod(url) {
          @Override
          public boolean getFollowRedirects() {
            return true;
          }
        };
      } else {
        post = new PostMethod(url);
      }
      // we can't use post.setFollowRedirects(true) as it will throw
      // IllegalArgumentException:
      // Entity enclosing requests cannot be redirected without user
      // intervention
      setLoginHeader(post);

      // NUTCH-2280
      LOGGER.debug("FormAuth: set cookie policy");
      this.setCookieParams(authConfigurer, post.getParams());

      post.addParameters(params.toArray(new NameValuePair[0]));
      int rspCode = client.executeMethod(post);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("rspCode: " + rspCode);
        LOGGER.debug("\nSending 'POST' request to URL : " + url);

        LOGGER.debug("Post parameters : " + params);
        LOGGER.debug("Response Code : " + rspCode);
        for (Header header : post.getRequestHeaders()) {
          LOGGER.debug("Response headers : " + header);
        }
      }
      String rst = IOUtils.toString(post.getResponseBodyAsStream());
      LOGGER.debug("login post result: " + rst);
    } finally {
      if (post != null) {
        post.releaseConnection();
      }
    }
  }

  /**
   * NUTCH-2280 Set the cookie policy value from httpclient-auth.xml for the
   * Post httpClient action.
   * 
   * @param fromConfigurer
   *          - the httpclient-auth.xml values
   * 
   * @param params
   *          - the HttpMethodParams from the current httpclient instance
   * 
   * @throws NoSuchFieldException
   * @throws SecurityException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   */
  private void setCookieParams(HttpFormAuthConfigurer formConfigurer,
      HttpMethodParams params) throws NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException {
    // NUTCH-2280 - set the HttpClient cookie policy
    if (formConfigurer.getCookiePolicy() != null) {
      String policy = formConfigurer.getCookiePolicy();
      Object p = FieldUtils.readDeclaredStaticField(CookiePolicy.class, policy);
      if (null != p) {
        LOGGER.debug("reflection of cookie value: " + p.toString());
        params.setParameter(HttpMethodParams.COOKIE_POLICY, p);
      }
    }
  }

  private void setLoginHeader(PostMethod post) {
    Map<String, String> headers = new HashMap<String, String>();
    headers.putAll(defaultLoginHeaders);
    // additionalPostHeaders can overwrite value in defaultLoginHeaders
    headers.putAll(authConfigurer.getAdditionalPostHeaders());
    for (Entry<String, String> entry : headers.entrySet()) {
      post.addRequestHeader(entry.getKey(), entry.getValue());
    }
    post.addRequestHeader("Cookie", getCookies());
  }

  private String httpGetPageContent(String url) throws IOException {

    GetMethod get = new GetMethod(url);
    try {
      for (Entry<String, String> entry : authConfigurer
          .getAdditionalPostHeaders().entrySet()) {
        get.addRequestHeader(entry.getKey(), entry.getValue());
      }
      client.executeMethod(get);
      Header cookieHeader = get.getResponseHeader("Set-Cookie");
      if (cookieHeader != null) {
        setCookies(cookieHeader.getValue());
      }
      String rst = IOUtils.toString(get.getResponseBodyAsStream());
      return rst;
    } finally {
      get.releaseConnection();
    }

  }

  private List<NameValuePair> getLoginFormParams(String pageContent)
      throws UnsupportedEncodingException {
    List<NameValuePair> params = new ArrayList<NameValuePair>();
    Document doc = Jsoup.parse(pageContent);
    Element loginform = doc.getElementById(authConfigurer.getLoginFormId());
    if (loginform == null) {
      LOGGER.debug("No form element found with 'id' = {}, trying 'name'.",
          authConfigurer.getLoginFormId());
      loginform = doc
          .select("form[name=" + authConfigurer.getLoginFormId() + "]").first();
      if (loginform == null) {
        LOGGER.debug("No form element found with 'name' = {}",
            authConfigurer.getLoginFormId());
        throw new IllegalArgumentException(
            "No form exists: " + authConfigurer.getLoginFormId());
      }
    }
    Elements inputElements = loginform.getElementsByTag("input");
    // skip fields in removedFormFields or loginPostData
    for (Element inputElement : inputElements) {
      String key = inputElement.attr("name");
      String value = inputElement.attr("value");
      if (authConfigurer.getLoginPostData().containsKey(key)
          || authConfigurer.getRemovedFormFields().contains(key)) {
        // value = loginPostData.get(key);
        continue;
      }
      params.add(new NameValuePair(key, value));
    }
    // add key and value in loginPostData
    for (Entry<String, String> entry : authConfigurer.getLoginPostData()
        .entrySet()) {
      params.add(new NameValuePair(entry.getKey(), entry.getValue()));
    }
    return params;
  }

  public String getCookies() {
    return cookies;
  }

  public void setCookies(String cookies) {
    this.cookies = cookies;
  }

  public boolean isRedirect() {
    return authConfigurer.isLoginRedirect();
  }

  public void setRedirect(boolean redirect) {
    this.authConfigurer.setLoginRedirect(redirect);
  }

}

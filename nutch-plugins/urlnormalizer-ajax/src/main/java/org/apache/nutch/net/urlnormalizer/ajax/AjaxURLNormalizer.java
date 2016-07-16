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

package org.apache.nutch.net.urlnormalizer.ajax;

import java.net.URL;
import java.net.URI;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.net.MalformedURLException;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nutch.net.URLNormalizer;
import org.apache.nutch.net.URLNormalizers;
import org.apache.hadoop.conf.Configuration;

/**
 * URLNormalizer capable of dealing with AJAX URL's.
 *
 * Use the following regex filter to prevent escaped fragments from being fetched.
 * ^(.*)\?.*_escaped_fragment_
 */
public class AjaxURLNormalizer implements URLNormalizer {
  public static final Logger LOG = LoggerFactory.getLogger(AjaxURLNormalizer.class);

  public static String AJAX_URL_PART = "#!";
  public static String ESCAPED_URL_PART = "_escaped_fragment_=";

  private Configuration conf;
  private Charset utf8;

  /**
   * Default constructor.
   */
  public AjaxURLNormalizer() {
    utf8 = Charset.forName("UTF-8");
  }

  /**
   * Attempts to normalize the input URL string
   *
   * @param String urlString
   * @return String
   */
  public String normalize(String urlString, String scope) throws MalformedURLException {
    LOG.info(scope + " // " + urlString);
  
    // When indexing, transform _escaped_fragment_ URL's to their #! counterpart
    if (scope.equals(URLNormalizers.SCOPE_INDEXER) && urlString.contains(ESCAPED_URL_PART)) {
      return normalizeEscapedFragment(urlString);
    }
    
    // Otherwise transform #! URL's to their _escaped_fragment_ counterpart
    if (urlString.contains(AJAX_URL_PART)) {
      LOG.info(scope + " // " + normalizeHashedFragment(urlString));
      return normalizeHashedFragment(urlString);
    }

    // Nothing to normalize here, return verbatim
    return urlString;
  }

  /**
   * Returns a normalized input URL. #! querystrings are transformed
   * to a _escaped_fragment_ form.
   *
   * @param String urlString
   * @return String
   */
  protected String normalizeHashedFragment(String urlString) throws MalformedURLException {
    URL u = new URL(urlString);
    int pos = urlString.indexOf(AJAX_URL_PART);
    StringBuilder sb = new StringBuilder(urlString.substring(0, pos));

    // Get the escaped fragment
    String escapedFragment = escape(urlString.substring(pos + AJAX_URL_PART.length()));

    // Check if we already have a query in the URL
    if (u.getQuery() == null) {
      sb.append("?");
    } else {
      sb.append("&");
    }

    // Append the escaped fragment key and the value
    sb.append(ESCAPED_URL_PART);
    sb.append(escapedFragment);

    return sb.toString();
  }

  /**
   * Returns a normalized input URL. _escaped_fragment_ querystrings are
   * transformed to a #! form.
   *
   * @param String urlString
   * @return String
   */
  protected String normalizeEscapedFragment(String urlString) throws MalformedURLException {
    int pos = urlString.indexOf(ESCAPED_URL_PART);
    URL u = new URL(urlString);
    StringBuilder sb = new StringBuilder();

    // Write the URL without query string, we'll handle that later
    sb.append(u.getProtocol());
    sb.append("://");
    sb.append(u.getHost());
    if (u.getPort() != -1) {
      sb.append(":");
      sb.append(u.getPort());
    }
    sb.append(u.getPath());

    // Get the query string
    String queryString = u.getQuery();

    // Check if there's an & in the query string
    int ampPos = queryString.indexOf("&");
    String keyValuePair = null;

    // If there's none, then the escaped fragment is the only k/v pair
    if (ampPos == -1) {
      keyValuePair = queryString;
      queryString = "";
    } else {
      // Obtain the escaped k/v pair
      keyValuePair = queryString.substring(ampPos + 1);

      // Remove the escaped fragment key/value pair from the query string
      queryString = queryString.replaceFirst("&" + keyValuePair, "");
    }

    // Remove escapedUrlPart from the keyValuePair
    keyValuePair = keyValuePair.replaceFirst(ESCAPED_URL_PART, "");

    // Get the fragment escaped
    String unescapedFragment = unescape(keyValuePair);

    // Append a possible query string, without original escaped fragment
    if (queryString.length() > 0) {
      sb.append("?");
      sb.append(queryString);
    }

    // Append the fragment delimiter and the unescaped fragment
    sb.append("#!");
    sb.append(unescapedFragment);

    return sb.toString();
  }

  /**
   * Unescape some exotic characters in the fragment part
   *
   * @param String fragmentPart
   * @return String
   */
  protected String unescape(String fragmentPart) {
    try {
      fragmentPart = URLDecoder.decode(fragmentPart, "UTF-8");
    } catch (Exception e) {
      /// bluh
    }

    return fragmentPart;
  }

  /**
   * Escape some exotic characters in the fragment part
   *
   * @param String fragmentPart
   * @return String
   */
  protected String escape(String fragmentPart) {
    String hex = null;
    StringBuilder sb = new StringBuilder(fragmentPart.length());

    for (byte b : fragmentPart.getBytes(utf8)) {
      if (b < 33) {
        sb.append('%');

        hex = Integer.toHexString(b & 0xFF).toUpperCase();

        // Prevent odd # chars
        if (hex.length() % 2 != 0) {
          sb.append('0');
        }
        sb.append(hex);
      } else if (b == 35) {
        sb.append("%23");
      } else if (b == 37) {
        sb.append("%25");
      } else if (b == 38) {
        sb.append("%26");
      } else if (b == 43) {
        sb.append("%2B");
      } else {
        sb.append((char)b);
      }
    }

    return sb.toString();
  }

  /**
   * @param Configuration conf
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * @return Configuration
   */
  public Configuration getConf() {
    return this.conf;
  }

}
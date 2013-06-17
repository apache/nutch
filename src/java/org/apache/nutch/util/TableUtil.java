/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.nutch.util.StringUtil;

public class TableUtil {

  public static final ByteBuffer YES_VAL = ByteBuffer.wrap(new byte[] { 'y' });

  /**
   * Reverses a url's domain. This form is better for storing in hbase. Because
   * scans within the same domain are faster.
   * <p>
   * E.g. "http://bar.foo.com:8983/to/index.html?a=b" becomes
   * "com.foo.bar:8983:http/to/index.html?a=b".
   *
   * @param url
   *          url to be reversed
   * @return Reversed url
   * @throws MalformedURLException
   */
  public static String reverseUrl(String urlString)
      throws MalformedURLException {
    return reverseUrl(new URL(urlString));
  }

  /**
   * Reverses a url's domain. This form is better for storing in hbase. Because
   * scans within the same domain are faster.
   * <p>
   * E.g. "http://bar.foo.com:8983/to/index.html?a=b" becomes
   * "com.foo.bar:http:8983/to/index.html?a=b".
   *
   * @param url
   *          url to be reversed
   * @return Reversed url
   */
  public static String reverseUrl(URL url) {
    String host = url.getHost();
    String file = url.getFile();
    String protocol = url.getProtocol();
    int port = url.getPort();

    StringBuilder buf = new StringBuilder();

    /* reverse host */
    reverseAppendSplits(host, buf);

    /* add protocol */
    buf.append(':');
    buf.append(protocol);

    /* add port if necessary */
    if (port != -1) {
      buf.append(':');
      buf.append(port);
    }

    /* add path */
    if (file.length() > 0 && '/' != file.charAt(0)) {
      buf.append('/');
    }
    buf.append(file);

    return buf.toString();
  }

  public static String unreverseUrl(String reversedUrl) {
    StringBuilder buf = new StringBuilder(reversedUrl.length() + 2);

    int pathBegin = reversedUrl.indexOf('/');
    if (pathBegin == -1)
      pathBegin = reversedUrl.length();
    String sub = reversedUrl.substring(0, pathBegin);

    String[] splits = StringUtils.splitPreserveAllTokens(sub, ':'); // {<reversed host>, <port>, <protocol>}
    
    buf.append(splits[1]); // add protocol
    buf.append("://");
    reverseAppendSplits(splits[0], buf); // splits[0] is reversed
    // host
    if (splits.length == 3) { // has a port
      buf.append(':');
      buf.append(splits[2]);
    }
    buf.append(reversedUrl.substring(pathBegin));
    return buf.toString();
  }

  /**
   * Given a reversed url, returns the reversed host E.g
   * "com.foo.bar:http:8983/to/index.html?a=b" -> "com.foo.bar"
   *
   * @param reversedUrl
   *          Reversed url
   * @return Reversed host
   */
  public static String getReversedHost(String reversedUrl) {
    return reversedUrl.substring(0, reversedUrl.indexOf(':'));
  }

  private static void reverseAppendSplits(String string, StringBuilder buf) {
    String[] splits = StringUtils.split(string,'.');
    if (splits.length > 0) {
      for (int i = splits.length - 1; i > 0; i--) {
        buf.append(splits[i]);
        buf.append('.');
      }
      buf.append(splits[0]);
    } else {
      buf.append(string);
    }
  }

  public static String reverseHost(String hostName) {
    StringBuilder buf = new StringBuilder();
    reverseAppendSplits(hostName, buf);
    return buf.toString();
    
  }
  public static String unreverseHost(String reversedHostName) {
    return reverseHost(reversedHostName); // Reversible
  }
  
  
  /**
   * Convert given Utf8 instance to String and and cleans out 
   * any offending "�" from the String.
   *
   * @param utf8
   *          Utf8 object
   * @return string-ifed Utf8 object or null if Utf8 instance is null
   */
  public static String toString(Utf8 utf8) {
    return (utf8 == null ? null : StringUtil.cleanField(utf8.toString()));
  }

}

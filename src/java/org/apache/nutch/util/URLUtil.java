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

package org.apache.nutch.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;

import org.apache.nutch.util.domain.DomainSuffix;
import org.apache.nutch.util.domain.DomainSuffixes;

/** Utility class for URL analysis */
public class URLUtil {

  private static Pattern IP_PATTERN = Pattern.compile("(\\d{1,3}\\.){3}(\\d{1,3})");

  /** Returns the domain name of the url. The domain name of a url is
   *  the substring of the url's hostname, w/o subdomain names. As an
   *  example <br><code>
   *  getDomainName(conf, new URL(http://lucene.apache.org/))
   *  </code><br>
   *  will return <br><code> apache.org</code>
   *   */
  public static String getDomainName(URL url) {
    DomainSuffixes tlds = DomainSuffixes.getInstance();
    String host = url.getHost();
    //it seems that java returns hostnames ending with .
    if(host.endsWith("."))
      host = host.substring(0, host.length() - 1);
    if(IP_PATTERN.matcher(host).matches())
      return host;
    
    int index = 0;
    String candidate = host;
    for(;index >= 0;) {
      index = candidate.indexOf('.');
      String subCandidate = candidate.substring(index+1); 
      if(tlds.isDomainSuffix(subCandidate)) {
        return candidate; 
      }
      candidate = subCandidate;
    }
    return candidate;
  }

  /** Returns the domain name of the url. The domain name of a url is
   *  the substring of the url's hostname, w/o subdomain names. As an
   *  example <br><code>
   *  getDomainName(conf, new http://lucene.apache.org/)
   *  </code><br>
   *  will return <br><code> apache.org</code>
   * @throws MalformedURLException
   */
  public static String getDomainName(String url) throws MalformedURLException {
    return getDomainName(new URL(url));
  }

  /** Returns whether the given urls have the same domain name.
   * As an example, <br>
   * <code> isSameDomain(new URL("http://lucene.apache.org")
   * , new URL("http://people.apache.org/"))
   * <br> will return true. </code>
   *
   * @return true if the domain names are equal
   */
  public static boolean isSameDomainName(URL url1, URL url2) {
    return getDomainName(url1).equalsIgnoreCase(getDomainName(url2));
  }

  /**Returns whether the given urls have the same domain name.
  * As an example, <br>
  * <code> isSameDomain("http://lucene.apache.org"
  * ,"http://people.apache.org/")
  * <br> will return true. </code>
  * @return true if the domain names are equal
  * @throws MalformedURLException
  */
  public static boolean isSameDomainName(String url1, String url2)
    throws MalformedURLException {
    return isSameDomainName(new URL(url1), new URL(url2));
  }

  /** Returns the {@link DomainSuffix} corresponding to the
   * last public part of the hostname
   */
  public static DomainSuffix getDomainSuffix(URL url) {
    DomainSuffixes tlds = DomainSuffixes.getInstance();
    String host = url.getHost();
    if(IP_PATTERN.matcher(host).matches())
      return null;
    
    int index = 0;
    String candidate = host;
    for(;index >= 0;) {
      index = candidate.indexOf('.');
      String subCandidate = candidate.substring(index+1);
      DomainSuffix d = tlds.get(subCandidate);
      if(d != null) {
        return d; 
      }
      candidate = subCandidate;
    }
    return null;
  }

  /** Returns the {@link DomainSuffix} corresponding to the
   * last public part of the hostname
   */
  public static DomainSuffix getDomainSuffix(String url) throws MalformedURLException {
    return getDomainSuffix(new URL(url));
  }

  /** Partitions of the hostname of the url by "."  */
  public static String[] getHostSegments(URL url) {
    String host = url.getHost();
    //return whole hostname, if it is an ipv4
    //TODO : handle ipv6
    if(IP_PATTERN.matcher(host).matches())
      return new String[] {host};
    return host.split("\\.");
  }

  /** Partitions of the hostname of the url by "."
   * @throws MalformedURLException */
  public static String[] getHostSegments(String url) throws MalformedURLException {
   return getHostSegments(new URL(url));
  }

  /** Given two urls (source and destination of the redirect),
   * returns the representative one.
   *
   * <p>Implements the algorithm described here:
   * <br>
   * <a href="http://help.yahoo.com/l/nz/yahooxtra/search/webcrawler/slurp-11.html">
   * How does the Yahoo! webcrawler handle redirects?</a>
   * <br><br>
   * The algorithm is as follows:
   * <ol>
   *  <li>Choose target url if either url is malformed.</li>
   *  <li>When a page in one domain redirects to a page in another domain,
   *  choose the "target" URL.</li>
   *  <li>When a top-level page in a domain presents a permanent redirect
   *  to a page deep within the same domain, choose the "source" URL.</li>
   *  <li>When a page deep within a domain presents a permanent redirect
   *  to a page deep within the same domain, choose the "target" URL.</li>
   *  <li>When a page in a domain presents a temporary redirect to
   *  another page in the same domain, choose the "source" URL.<li>
   * <ol>
   * </p>
   *
   * @param src Source url of redirect
   * @param dst Destination url of redirect
   * @param temp Flag to indicate if redirect is temporary
   * @return Representative url (either src or dst)
   */
  public static String chooseRepr(String src, String dst, boolean temp) {
    URL srcUrl;
    URL dstUrl;
    try {
      srcUrl = new URL(src);
      dstUrl = new URL(dst);
    } catch (MalformedURLException e) {
      return dst;
    }

    String srcDomain = URLUtil.getDomainName(srcUrl);
    String dstDomain = URLUtil.getDomainName(dstUrl);

    if (!srcDomain.equals(dstDomain)) {
      return dst;
    }

    String srcFile = srcUrl.getFile();

    if (!temp && srcFile.equals("/")) {
      return src;
    }

    return temp ? src : dst;
  }

  /** For testing */
  public static void main(String[] args){
    
    if(args.length!=1) {
      System.err.println("Usage : URLUtil <url>");
      return ;
    }
    
    String url = args[0];
    try {
      System.out.println(URLUtil.getDomainName(new URL(url)));
    }
    catch (MalformedURLException ex) {
      ex.printStackTrace();
    }
  }
}

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
import java.net.*;
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
  public static String[] getHostBatches(URL url) {
    String host = url.getHost();
    //return whole hostname, if it is an ipv4
    //TODO : handle ipv6
    if(IP_PATTERN.matcher(host).matches())
      return new String[] {host};
    return host.split("\\.");
  }

  /** Partitions of the hostname of the url by "."
   * @throws MalformedURLException */
  public static String[] getHostBatches(String url) throws MalformedURLException {
   return getHostBatches(new URL(url));
  }

  /**
   * <p>Given two urls, a src and a destination of a redirect, it returns the 
   * representative url.<p>
   * 
   * <p>This method implements an extended version of the algorithm used by the
   * Yahoo! Slurp crawler described here:<br>
   * <a href=
   * "http://help.yahoo.com/l/nz/yahooxtra/search/webcrawler/slurp-11.html"> How
   * does the Yahoo! webcrawler handle redirects?</a> <br>
   * <br>
   * <ol>
   * <li>Choose target url if either url is malformed.</li>
   * <li>If different domains the keep the destination whether or not the 
   * redirect is temp or perm</li>
   * <ul><li>a.com -> b.com*</li></ul>
   * <li>If the redirect is permanent and the source is root, keep the source.</li>
   * <ul><li>*a.com -> a.com?y=1 || *a.com -> a.com/xyz/index.html</li></ul>
   * <li>If the redirect is permanent and the source is not root and the 
   * destination is root, keep the destination</li>
   * <ul><li>a.com/xyz/index.html -> a.com*</li></ul>
   * <li>If the redirect is permanent and neither the source nor the destination
   * is root, then keep the destination</li>
   * <ul><li>a.com/xyz/index.html -> a.com/abc/page.html*</li></ul>
   * <li>If the redirect is temporary and source is root and destination is not
   * root, then keep the source</li>
   * <ul><li>*a.com -> a.com/xyz/index.html</li></ul>
   * <li>If the redirect is temporary and source is not root and destination is
   * root, then keep the destination</li>
   * <ul><li>a.com/xyz/index.html -> a.com*</li></ul>
   * <li>If the redirect is temporary and neither the source or the destination
   * is root, then keep the shortest url.  First check for the shortest host,
   * and if both are equal then check by path.  Path is first by length then by
   * the number of / path separators.</li>
   * <ul>
   * <li>a.com/xyz/index.html -> a.com/abc/page.html*</li>
   * <li>*www.a.com/xyz/index.html -> www.news.a.com/xyz/index.html</li>
   * </ul>
   * <li>If the redirect is temporary and both the source and the destination
   * are root, then keep the shortest sub-domain</li>
   * <ul><li>*www.a.com -> www.news.a.com</li></ul>
   * <br>
   * While not in this logic there is a further piece of representative url 
   * logic that occurs during indexing and after scoring.  During creation of 
   * the basic fields before indexing, if a url has a representative url stored
   * we check both the url and its representative url (which should never be 
   * the same) against their linkrank scores and the highest scoring one is 
   * kept as the url and the lower scoring one is held as the orig url inside 
   * of the index.
   * 
   * @param src The source url.
   * @param dst The destination url.
   * @param temp Is the redirect a temporary redirect.
   * 
   * @return String The representative url.
   */
  public static String chooseRepr(String src, String dst, boolean temp) {

    // validate both are well formed urls
    URL srcUrl;
    URL dstUrl;
    try {
      srcUrl = new URL(src);
      dstUrl = new URL(dst);
    }
    catch (MalformedURLException e) {
      return dst;
    }

    // get the source and destination domain, host, and page
    String srcDomain = URLUtil.getDomainName(srcUrl);
    String dstDomain = URLUtil.getDomainName(dstUrl);
    String srcHost = srcUrl.getHost();
    String dstHost = dstUrl.getHost();
    String srcFile = srcUrl.getFile();
    String dstFile = dstUrl.getFile();

    // are the source and destination the root path url.com/ or url.com
    boolean srcRoot = (srcFile.equals("/") || srcFile.length() == 0);
    boolean destRoot = (dstFile.equals("/") || dstFile.length() == 0);

    // 1) different domain them keep dest, temp or perm
    // a.com -> b.com*
    //    
    // 2) permanent and root, keep src
    // *a.com -> a.com?y=1 || *a.com -> a.com/xyz/index.html
    //      
    // 3) permanent and not root and dest root, keep dest
    // a.com/xyz/index.html -> a.com*
    //      
    // 4) permanent and neither root keep dest
    // a.com/xyz/index.html -> a.com/abc/page.html*
    //      
    // 5) temp and root and dest not root keep src
    // *a.com -> a.com/xyz/index.html
    //  
    // 7) temp and not root and dest root keep dest
    // a.com/xyz/index.html -> a.com*
    //  
    // 8) temp and neither root, keep shortest, if hosts equal by path else by
    // hosts. paths are first by length then by number of / separators
    // a.com/xyz/index.html -> a.com/abc/page.html*
    // *www.a.com/xyz/index.html -> www.news.a.com/xyz/index.html
    //  
    // 9) temp and both root keep shortest sub domain
    // *www.a.com -> www.news.a.com

    // if we are dealing with a redirect from one domain to another keep the
    // destination
    if (!srcDomain.equals(dstDomain)) {
      return dst;
    }

    // if it is a permanent redirect
    if (!temp) {
      
      // if source is root return source, otherwise destination
      if (srcRoot) {
        return src;
      }
      else {
        return dst;
      }
    }
    else { // temporary redirect

      // source root and destination not root
      if (srcRoot && !destRoot) {
        return src;
      }
      else if (!srcRoot && destRoot) { // destination root and source not
        return dst;
      }
      else if (!srcRoot && !destRoot && (srcHost.equals(dstHost))) {

        // source and destination hosts are the same, check paths, host length
        int numSrcPaths = srcFile.split("/").length;
        int numDstPaths = dstFile.split("/").length;
        if (numSrcPaths != numDstPaths) {
          return (numDstPaths < numSrcPaths ? dst : src);
        }
        else {
          int srcPathLength = srcFile.length();
          int dstPathLength = dstFile.length();
          return (dstPathLength < srcPathLength ? dst : src);
        }
      }
      else {

        // different host names and both root take the shortest
        int numSrcSubs = srcHost.split("\\.").length;
        int numDstSubs = dstHost.split("\\.").length;
        return (numDstSubs < numSrcSubs ? dst : src);
      }
    }
  }

  /**
   * Returns the lowercased hostname for the url or null if the url is not well
   * formed.
   * 
   * @param url The url to check.
   * @return String The hostname for the url.
   */
  public static String getHost(String url) {
    try {
      return new URL(url).getHost().toLowerCase();
    }
    catch (MalformedURLException e) {
      return null;
    }
  }

  /**
   * Returns the page for the url.  The page consists of the protocol, host,
   * and path, but does not include the query string.  The host is lowercased
   * but the path is not.
   * 
   * @param url The url to check.
   * @return String The page for the url.
   */
  public static String getPage(String url) {
    try {
      // get the full url, and replace the query string with and empty string
      url = url.toLowerCase();
      String queryStr = new URL(url).getQuery();
      return (queryStr != null) ? url.replace("?" + queryStr, "") : url;
    }
    catch (MalformedURLException e) {
      return null;
    }
  }
  
  public static String toASCII(String url) {
    try {
      URL u = new URL(url);
      URI p = new URI(u.getProtocol(),
        null,
        IDN.toASCII(u.getHost()),
        u.getPort(),
        u.getPath(),
        u.getQuery(),
        u.getRef());

      return p.toString();
    }
    catch (Exception e) {
      return null;
    }
  }

  public static String toUNICODE(String url) {
    try {
      URL u = new URL(url);
      URI p = new URI(u.getProtocol(),
        null,
        IDN.toUnicode(u.getHost()),
        u.getPort(),
        u.getPath(),
        u.getQuery(),
        u.getRef());

      return p.toString();
    }
    catch (Exception e) {
      return null;
    }
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

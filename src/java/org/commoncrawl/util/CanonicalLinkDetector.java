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
package org.commoncrawl.util;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HeaderElement;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.message.BasicHeaderValueParser;
import org.apache.nutch.protocol.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CanonicalLinkDetector {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  protected static Set<String> SUPPORTED_CONTENT_TYPES = new HashSet<>();
  static {
    SUPPORTED_CONTENT_TYPES.add("text/html");
    SUPPORTED_CONTENT_TYPES.add("application/xhtml+xml");
  }

  /**
   * Pattern to match canonical link elements in HTML. The length of the
   * canonical link URL inside the element is limited to max. 2048 characters.
   */
  private static Pattern canonicalLinkPattern = Pattern.compile(
      "<link\\s+[^>]{0,2054}rel=(?:'canonical'|\"canonical\"|canonical\\b)[^>]{0,2054}>",
      Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
  private static Pattern hrefPattern = Pattern
      .compile("href=['\"]?([^'\"\\s]{0,2048})", Pattern.CASE_INSENSITIVE);

  private static Pattern canonicalRelValuePattern = Pattern
      .compile("\\bcanonical\\b", Pattern.CASE_INSENSITIVE);
  private static final Pattern linkInParentheses = Pattern
      .compile("^\\s*<\\s*(.*?)\\s*>\\s*$");

  private static final List<String> EMPTY_RESULT = List.of();

  /** top-N bytes of HTML to look for canonical link */
  private static int CHUNK_SIZE = 65536;

  /** max. number canonical links to detect */
  private static int MAX_LINKS = 1;

  /**
   * Extract canonical link from HTTP header.
   * 
   * The extraction is delegated to {@link BasicHeaderValueParser} because
   * parsing multi-valued link attributes is far from trivial, e.g.
   * 
   * <pre>
   Link: <https://frontera.library.ucla.edu/songs>; rel="canonical",<https://frontera.library.ucla.edu/songs>; rel="shortlink",<https://frontera.library.ucla.edu/favicon.ico>; rel="shortcut icon"
   * </pre>
   * 
   * @param &quot;Link&quot;
   *          header values
   * @return the canonical links found, or an empty list if no canonical link is
   *         found
   */
  protected static List<String> detectCanonicalLinksHttpHeader(
      String[] linkHeaders, int maxResults) {
    List<String> result = EMPTY_RESULT;
    for (String httpHeaderLink : linkHeaders) {
      HeaderElement elem;
      try {
        elem = BasicHeaderValueParser.parseHeaderElement(httpHeaderLink,
            BasicHeaderValueParser.INSTANCE);
      } catch (ParseException e) {
        LOG.error("Failed to parse Link HTTP header: {}", httpHeaderLink, e);
        continue;
      }
      for (NameValuePair param : elem.getParameters()) {
        if ("rel".equalsIgnoreCase(param.getName())
            && canonicalRelValuePattern.matcher(param.getValue()).find()) {
          String link = elem.getName();
          // match inside < ... >
          Matcher urlMatcher = linkInParentheses.matcher(link);
          if (urlMatcher.matches()) {
            link = urlMatcher.group(1);
            if (result == EMPTY_RESULT) {
              result = new ArrayList<String>(1);
            }
            result.add(link);
            if (result.size() >= maxResults) {
              break;
            }
          }
        }
      }
    }
    return result;
  }

  public static boolean isEligibleContentType(String contentType) {
    return SUPPORTED_CONTENT_TYPES.contains(contentType);
  }

  /**
   * Extract canonical link from HTTP header.
   * 
   * The extraction is delegated to {@link BasicHeaderValueParser} because
   * parsing multi-valued link attributes is far from trivial, e.g.
   * 
   * <pre>
   Link: <https://frontera.library.ucla.edu/songs>; rel="canonical",<https://frontera.library.ucla.edu/songs>; rel="shortlink",<https://frontera.library.ucla.edu/favicon.ico>; rel="shortcut icon"
   * </pre>
   * 
   * @param &quot;Link&quot;
   *          header values
   * @return the canonical links found, or an empty list if no canonical link is
   *         found
   */
  public static List<String> detectCanonicalLinksHTML(byte[] content, int chunkSize,
      int maxResults) {
    List<String> result = EMPTY_RESULT;
    int length = content.length < chunkSize ? content.length : chunkSize;
    CharSequence cs;
    cs = new ByteArrayCharSequence(content, length);
    Matcher clMatcher = canonicalLinkPattern.matcher(cs);
    while (clMatcher.find()) {
      CharSequence cls;
      cls = cs.subSequence(clMatcher.start(), clMatcher.end());
      Matcher hrefMatcher = hrefPattern.matcher(cls);
      if (hrefMatcher.find(5)) {
        String cl = hrefMatcher.group(1);
        if (result == EMPTY_RESULT) {
          result = new ArrayList<String>(1);
        }
        result.add(cl);
        if (result.size() >= maxResults) {
          break;
        }
      }
    }
    return result;
  }

  public static List<String> detectCanonicalLinks(Content content,
      int chunkSize, int maxLinks) {

    /*
     * Note: the HTTP header look-up is case-insensitive if
     * CaseInsensitiveMetadata or SpellCheckedMetadata is used.
     */
    String[] linkHeaders = content.getMetadata().getValues("Link");
    List<String> canonicalLinks = detectCanonicalLinksHttpHeader(linkHeaders,
        maxLinks);

    if (canonicalLinks.size() < maxLinks
        && isEligibleContentType(content.getContentType())) {
      List<String> linksHtml = detectCanonicalLinksHTML(content.getContent(), chunkSize, maxLinks);
      if (linksHtml.size() > 0) {
        if (canonicalLinks == EMPTY_RESULT) {
          canonicalLinks = linksHtml;
        } else {
          canonicalLinks.addAll(linksHtml);
        }
      }
    }
    return canonicalLinks;
  }

  public static List<String> detectCanonicalLinks(Content content) {
    return detectCanonicalLinks(content, CHUNK_SIZE, MAX_LINKS);
  }

}

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

package org.apache.nutch.net.urlnormalizer.basic;

import java.lang.invoke.MethodHandles;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.nutch.net.URLNormalizer;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts URLs to a normal form:
 * <ul>
 * <li>remove dot segments in path: <code>/./</code> or <code>/../</code></li>
 * <li>remove default ports, e.g. 80 for protocol <code>http://</code></li>
 * <li>normalize <a href=
 * "https://en.wikipedia.org/wiki/Percent-encoding#Percent-encoding_in_a_URI">
 * percent-encoding</a> in URL paths</li>
 * </ul>
 */
public class BasicURLNormalizer extends Configured implements URLNormalizer {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Pattern to detect whether a URL path could be normalized. Contains one of
   * /. or ./ /.. or ../ //
   */
  private final static Pattern hasNormalizablePathPattern = Pattern
      .compile("/[./]|[.]/");

  /**
   * Nutch 1098 - finds URL encoded parts of the URL
   */
  private final static Pattern unescapeRulePattern = Pattern
      .compile("%([0-9A-Fa-f]{2})");
  
  // charset used for encoding URLs before escaping
  private final static Charset utf8 = Charset.forName("UTF-8");

  /** look-up table for characters which should not be escaped in URL paths */
  private final static boolean[] unescapedCharacters = new boolean[128];
  static {
    for (int c = 0; c < 128; c++) {
      /* https://tools.ietf.org/html/rfc3986#section-2.2
       * For consistency, percent-encoded octets in the ranges of ALPHA
       * (%41-%5A and %61-%7A), DIGIT (%30-%39), hyphen (%2D), period (%2E),
       * underscore (%5F), or tilde (%7E) should not be created by URI
       * producers and, when found in a URI, should be decoded to their
       * corresponding unreserved characters by URI normalizers.
       */
      if ((0x41 <= c && c <= 0x5A)
        || (0x61 <= c && c <= 0x7A)
        || (0x30 <= c && c <= 0x39)
        || c == 0x2D || c == 0x2E
        || c == 0x5F || c == 0x7E) {
        unescapedCharacters[c] = true;
      } else {
        unescapedCharacters[c] = false;
      }
    }
  }

  public String normalize(String urlString, String scope)
      throws MalformedURLException {
    
    if ("".equals(urlString)) // permit empty
      return urlString;

    urlString = urlString.trim(); // remove extra spaces

    URL url = new URL(urlString);

    String protocol = url.getProtocol();
    String host = url.getHost();
    int port = url.getPort();
    String file = url.getFile();

    boolean changed = false;

    if (!urlString.startsWith(protocol)) // protocol was lowercased
      changed = true;

    if ("http".equals(protocol) || "https".equals(protocol)
        || "ftp".equals(protocol)) {

      if (host != null && url.getAuthority() != null) {
        String newHost = host.toLowerCase(Locale.ROOT); // lowercase host
        if (!host.equals(newHost)) {
          host = newHost;
          changed = true;
        } else if (!url.getAuthority().equals(newHost)) {
          // authority (http://<...>/) contains other elements (port, user,
          // etc.) which will likely cause a change if left away
          changed = true;
        }
      } else {
        // no host or authority: recompose the URL from components
        changed = true;
      }

      if (port == url.getDefaultPort()) { // uses default port
        port = -1; // so don't specify it
        changed = true;
      }

      if (file == null || "".equals(file)) { // add a slash
        file = "/";
        changed = true;
      }

      if (url.getRef() != null) { // remove the ref
        changed = true;
      }

      // check for unnecessary use of "/../", "/./", and "//"
      String file2 = getFileWithNormalizedPath(url);
      if (!file.equals(file2)) {
        changed = true;
        file = file2;
      }
    }

    // properly encode characters in path/file using percent-encoding
    String file2 = unescapePath(file);
    file2 = escapePath(file2);
    if (!file.equals(file2)) {
      changed = true;
      file = file2;
    }

    if (changed)
      urlString = new URL(protocol, host, port, file).toString();

    return urlString;
  }

  private String getFileWithNormalizedPath(URL url)
      throws MalformedURLException {
    String file;

    if (hasNormalizablePathPattern.matcher(url.getPath()).find()) {
      // only normalize the path if there is something to normalize
      // to avoid needless work
      try {
        file = url.toURI().normalize().toURL().getFile();
        // URI.normalize() does not normalize leading dot segments,
        // see also http://tools.ietf.org/html/rfc3986#section-5.2.4
        int start = 0;
        while (file.startsWith("/../", start)) {
          start += 3;
        }
        if (start > 0) {
          file = file.substring(start);
        }
      } catch (URISyntaxException e) {
        file = url.getFile();
      }
    } else {
      file = url.getFile();
    }

    // if path is empty return a single slash
    if (file.isEmpty()) {
      file = "/";
    }

    return file;
  }
  
  /**
   * Remove % encoding from path segment in URL for characters which should be
   * unescaped according to <a
   * href="https://tools.ietf.org/html/rfc3986#section-2.2">RFC3986</a>.
   */
  private String unescapePath(String path) {
    StringBuilder sb = new StringBuilder();
    
    Matcher matcher = unescapeRulePattern.matcher(path);
    
    int end = -1;
    int letter;

    // Traverse over all encoded groups
    while (matcher.find()) {
      // Append everything up to this group
      sb.append(path.substring(end + 1, matcher.start()));
      
      // Get the integer representation of this hexadecimal encoded character
      letter = Integer.valueOf(matcher.group().substring(1), 16);

      if (letter < 128 && unescapedCharacters[letter]) {
        // character should be unescaped in URLs
        sb.append(new Character((char)letter));
      } else {
        // Append the encoded character as uppercase
        sb.append(matcher.group().toUpperCase(Locale.ROOT));
      }
      
      end = matcher.start() + 2;
    }
    
    letter = path.length();
    
    // Append the rest if there's anything
    if (end <= letter - 1) {
      sb.append(path.substring(end + 1, letter));
    }

    // Ok!
    return sb.toString();
  }

  /**
   * Convert path segment of URL from Unicode to UTF-8 and escape all
   * characters which should be escaped according to <a
   * href="https://tools.ietf.org/html/rfc3986#section-2.2">RFC3986</a>..
   */
  private String escapePath(String path) {
    StringBuilder sb = new StringBuilder(path.length());

    // Traverse over all bytes in this URL
    for (byte b: path.getBytes(utf8)) {
      // Is this a control character?
      if (b < 0x21 || b == 0x5B || b == 0x5D || b == 0x7B || b == 0x7D) {
        // Start escape sequence 
        sb.append('%');
        
        // Get this byte's hexadecimal representation 
        String hex = Integer.toHexString(b & 0xFF).toUpperCase(Locale.ROOT);
        
        // Do we need to prepend a zero?
        if (hex.length() % 2 != 0 ) {
          sb.append('0');
          sb.append(hex);
        } else {
          // No, append this hexadecimal representation
          sb.append(hex);
        }
      } else {
        // No, just append this character as-is
        sb.append((char)b);
      }
    }
    
    return sb.toString();
  }

  public static void main(String args[]) throws IOException {
    BasicURLNormalizer normalizer = new BasicURLNormalizer();
    normalizer.setConf(NutchConfiguration.create());
    String scope = URLNormalizers.SCOPE_DEFAULT;
    if (args.length >= 1) {
      scope = args[0];
      System.out.println("Scope: " + scope);
    }
    String line, normUrl;
    BufferedReader in = new BufferedReader(
        new InputStreamReader(System.in, utf8));
    while ((line = in.readLine()) != null) {
      try {
        normUrl = normalizer.normalize(line, scope);
        System.out.println(normUrl);
      } catch (MalformedURLException e) {
        System.out.println("failed: " + line);
      }
    }
    System.exit(0);
  }

}

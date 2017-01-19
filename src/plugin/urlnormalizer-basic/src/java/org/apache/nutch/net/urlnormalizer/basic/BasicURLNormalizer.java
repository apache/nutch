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
import java.nio.charset.StandardCharsets;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.MalformedURLException;
import java.util.regex.Pattern;
import java.util.Locale;

// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Nutch imports
import org.apache.nutch.net.URLNormalizer;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/**
 * Converts URLs to a normal form:
 * <ul>
 * <li>remove dot segments in path: <code>/./</code> or <code>/../</code></li>
 * <li>remove default ports, e.g. 80 for protocol <code>http://</code></li>
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

      if (host != null) {
        String newHost = host.toLowerCase(Locale.ROOT); // lowercase host
        if (!host.equals(newHost)) {
          host = newHost;
          changed = true;
        } else if (!url.getAuthority().equals(newHost)) {
          // authority (http://<...>/) contains other elements (port, user,
          // etc.) which will likely cause a change if left away
          changed = true;
        }
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

  public static void main(String args[]) throws IOException {
    BasicURLNormalizer normalizer = new BasicURLNormalizer();
    normalizer.setConf(NutchConfiguration.create());
    String scope = URLNormalizers.SCOPE_DEFAULT;
    if (args.length >= 1) {
      scope = args[0];
      System.out.println("Scope: " + scope);
    }
    String line, normUrl;
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
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
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.urlfilter.validator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLFilter;

/**
 * <p>Validates URLs.</p>
 *
 * <p>Originally based in on php script by Debbie Dyer, validation.php v1.2b,
 * Date: 03/07/02,
 * http://javascript.internet.com. However, this validation now bears little
 * resemblance to the php original.</p>
 * <pre>
 *   Example of usage:
 *    UrlValidator urlValidator = UrlValidator.get();
 *    if (urlValidator.isValid("ftp://foo.bar.com/")) {
 *       System.out.println("url is valid");
 *    } else {
 *       System.out.println("url is invalid");
 *    }
 *
 *   prints out "url is valid"
 *  </pre>
 *
 * <p>Based on UrlValidator code from Apache commons-validator.</p>
 *
 * @see
 * <a href='http://www.ietf.org/rfc/rfc2396.txt' >
 *  Uniform Resource Identifiers (URI): Generic Syntax
 * </a>
 *
 */
public class UrlValidator implements URLFilter {

  private static final String ALPHA_CHARS = "a-zA-Z";

  private static final String ALPHA_NUMERIC_CHARS = ALPHA_CHARS + "\\d";

  private static final String SPECIAL_CHARS = ";/@&=,.?:+$";

  private static final String VALID_CHARS = "[^\\s" + SPECIAL_CHARS + "]";

  private static final String SCHEME_CHARS = ALPHA_CHARS;

  // Drop numeric, and  "+-." for now
  private static final String AUTHORITY_CHARS = ALPHA_NUMERIC_CHARS + "\\-\\.";

  private static final String ATOM = VALID_CHARS + '+';

  /**
   * This expression derived/taken from the BNF for URI (RFC2396).
   */
  private static final Pattern URL_PATTERN =
    Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)" +
                    "(\\?([^#]*))?(#(.*))?");

  /**
   * Schema/Protocol (ie. http:, ftp:, file:, etc).
   */
  private static final int PARSE_URL_SCHEME = 2;

  /**
   * Includes hostname/ip and port number.
   */
  private static final int PARSE_URL_AUTHORITY = 4;

  private static final int PARSE_URL_PATH = 5;

  private static final int PARSE_URL_QUERY = 7;

  /**
   * Protocol (ie. http:, ftp:,https:).
   */
  private static final Pattern SCHEME_PATTERN =
    Pattern.compile("^[" + SCHEME_CHARS + "]+");

  private static final Pattern AUTHORITY_PATTERN =
    Pattern.compile("^([" + AUTHORITY_CHARS + "]*)(:\\d*)?(.*)?");

  private static final int PARSE_AUTHORITY_HOST_IP = 1;

  private static final int PARSE_AUTHORITY_PORT = 2;

  /**
   * Should always be empty.
   */
  private static final int PARSE_AUTHORITY_EXTRA = 3;

  private static final Pattern PATH_PATTERN =
    Pattern.compile("^(/[-\\w:@&?=+,.!/~*'%$_;\\(\\)]*)?$");

  private static final Pattern QUERY_PATTERN = Pattern.compile("^(.*)$");

  private static final Pattern LEGAL_ASCII_PATTERN =
    Pattern.compile("^[\\x21-\\x7E]+$");

  private static final Pattern IP_V4_DOMAIN_PATTERN =
    Pattern.compile("^(\\d{1,3})[.](\\d{1,3})[.](\\d{1,3})[.](\\d{1,3})$");

  private static final Pattern DOMAIN_PATTERN =
    Pattern.compile("^" + ATOM + "(\\." + ATOM + ")*$");

  private static final Pattern PORT_PATTERN =
    Pattern.compile("^:(\\d{1,5})$");

  private static final Pattern ATOM_PATTERN =
    Pattern.compile("(" + ATOM + ")");

  private static final Pattern ALPHA_PATTERN =
    Pattern.compile("^[" + ALPHA_CHARS + "]");

  private Configuration conf;

  public String filter(String urlString) {
    return isValid(urlString) ? urlString : null;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * <p>Checks if a field has a valid url address.</p>
   *
   * @param value The value validation is being performed on.
   * A <code>null</code> value is considered invalid.
   * @return true if the url is valid.
   */
  private boolean isValid(String value) {
    if (value == null) {
      return false;
    }

    Matcher matchUrlPat = URL_PATTERN.matcher(value);
    if (!LEGAL_ASCII_PATTERN.matcher(value).matches()) {
      return false;
    }

    // Check the whole url address structure
    if (!matchUrlPat.matches()) {
      return false;
    }

    if (!isValidScheme(matchUrlPat.group(PARSE_URL_SCHEME))) {
      return false;
    }

    if (!isValidAuthority(matchUrlPat.group(PARSE_URL_AUTHORITY))) {
      return false;
    }

    if (!isValidPath(matchUrlPat.group(PARSE_URL_PATH))) {
      return false;
    }

    if (!isValidQuery(matchUrlPat.group(PARSE_URL_QUERY))) {
      return false;
    }

    return true;
  }

  /**
   * Validate scheme. If schemes[] was initialized to a non null,
   * then only those scheme's are allowed.  Note this is slightly different
   * than for the constructor.
   * @param scheme The scheme to validate.  A <code>null</code> value is
   * considered invalid.
   * @return true if valid.
   */
  private boolean isValidScheme(String scheme) {
    if (scheme == null) {
      return false;
    }

    return SCHEME_PATTERN.matcher(scheme).matches();
  }

  /**
   * Returns true if the authority is properly formatted.  An authority is
   * the combination of hostname and port.  A <code>null</code> authority
   * value is considered invalid.
   * @param authority Authority value to validate.
   * @return true if authority (hostname and port) is valid.
   */
  private boolean isValidAuthority(String authority) {
    if (authority == null) {
      return false;
    }

    Matcher authorityMatcher = AUTHORITY_PATTERN.matcher(authority);
    if (!authorityMatcher.matches()) {
      return false;
    }

    boolean ipV4Address = false;
    boolean hostname = false;
    // check if authority is IP address or hostname
    String hostIP = authorityMatcher.group(PARSE_AUTHORITY_HOST_IP);
    Matcher matchIPV4Pat = IP_V4_DOMAIN_PATTERN.matcher(hostIP);
    ipV4Address = matchIPV4Pat.matches();

    if (ipV4Address) {
      // this is an IP address so check components
      for (int i = 1; i <= 4; i++) {
        String ipSegment = matchIPV4Pat.group(i);
        if (ipSegment == null || ipSegment.length() <= 0) {
          return false;
        }

        try {
          if (Integer.parseInt(ipSegment) > 255) {
            return false;
          }
        } catch(NumberFormatException e) {
          return false;
        }

      }
    } else {
      // Domain is hostname name
      hostname = DOMAIN_PATTERN.matcher(hostIP).matches();
    }

    // rightmost hostname will never start with a digit.
    if (hostname) {
      // LOW-TECH FIX FOR VALIDATOR-202
      // TODO: Rewrite to use ArrayList and .add semantics: see VALIDATOR-203
      char[] chars = hostIP.toCharArray();
      int size = 1;
      for(int i=0; i<chars.length; i++) {
        if(chars[i] == '.') {
          size++;
        }
      }
      String[] domainSegment = new String[size];
      int segCount = 0;
      int segLen = 0;
      Matcher atomMatcher = ATOM_PATTERN.matcher(hostIP);

      while (atomMatcher.find()) {
        domainSegment[segCount] = atomMatcher.group();
        segLen = domainSegment[segCount].length() + 1;
        hostIP = (segLen >= hostIP.length()) ? ""
                                             : hostIP.substring(segLen);
        segCount++;
      }
      String topLevel = domainSegment[segCount - 1];
      if (topLevel.length() < 2 || topLevel.length() > 4) {
        return false;
      }

      // First letter of top level must be a alpha
      if (!ALPHA_PATTERN.matcher(topLevel.substring(0, 1)).matches()) {
        return false;
      }

      // Make sure there's a host name preceding the authority.
      if (segCount < 2) {
        return false;
      }
    }

    if (!hostname && !ipV4Address) {
      return false;
    }

    String port = authorityMatcher.group(PARSE_AUTHORITY_PORT);
    if (port != null) {
      if (!PORT_PATTERN.matcher(port).matches()) {
        return false;
      }
    }

    String extra = authorityMatcher.group(PARSE_AUTHORITY_EXTRA);
    return isBlankOrNull(extra);
  }

  /**
   * <p>Checks if the field isn't null and length of the field is greater
   * than zero not including whitespace.</p>
   *
   * @param value The value validation is being performed on.
   * @return true if blank or null.
   */
  private boolean isBlankOrNull(String value) {
    return ((value == null) || (value.trim().length() == 0));
  }

  /**
   * Returns true if the path is valid.  A <code>null</code> value is
   * considered invalid.
   * @param path Path value to validate.
   * @return true if path is valid.
   */
  private boolean isValidPath(String path) {
    if (path == null) {
      return false;
    }

    if (!PATH_PATTERN.matcher(path).matches()) {
      return false;
    }

    int slash2Count = countToken("//", path);
    int slashCount = countToken("/", path);
    int dot2Count = countToken("..", path);

    return (dot2Count <= 0) || ((slashCount - slash2Count - 1) > dot2Count);
  }

  /**
   * Returns true if the query is null or it's a properly formatted query
   * string.
   * @param query Query value to validate.
   * @return true if query is valid.
   */
  private boolean isValidQuery(String query) {
    if (query == null) {
      return true;
    }

    return QUERY_PATTERN.matcher(query).matches();
  }

  /**
   * Returns the number of times the token appears in the target.
   * @param token Token value to be counted.
   * @param target Target value to count tokens in.
   * @return the number of tokens.
   */
  private int countToken(String token, String target) {
    int tokenIndex = 0;
    int count = 0;
    while (tokenIndex != -1) {
      tokenIndex = target.indexOf(token, tokenIndex);
      if (tokenIndex > -1) {
        tokenIndex++;
        count++;
      }
    }
    return count;
  }

}

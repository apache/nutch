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
package org.apache.nutch.net;

import org.apache.oro.text.perl.Perl5Util;

/**
 * <p>Validates URLs.</p>
 *
 * <p>Originally based in on php script by Debbie Dyer, validation.php v1.2b, Date: 03/07/02,
 * http://javascript.internet.com. However, this validation now bears little resemblance
 * to the php original.</p>
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
public class UrlValidator {

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
  private static final String URL_PATTERN =
    "/^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?/";
  //                                                                      12            3  4          5       6   7        8 9

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
  private static final String SCHEME_PATTERN = "/^[" + SCHEME_CHARS + "]/";

  private static final String AUTHORITY_PATTERN =
    "/^([" + AUTHORITY_CHARS + "]*)(:\\d*)?(.*)?/";
  //                                                                            1                          2  3       4

  private static final int PARSE_AUTHORITY_HOST_IP = 1;

  private static final int PARSE_AUTHORITY_PORT = 2;

  /**
   * Should always be empty.
   */
  private static final int PARSE_AUTHORITY_EXTRA = 3;

  private static final String PATH_PATTERN = "/^(/[-\\w:@&?=+,.!/~*'%$_;]*)?$/";

  private static final String QUERY_PATTERN = "/^(.*)$/";

  private static final String LEGAL_ASCII_PATTERN = "/^[\\000-\\177]+$/";

  private static final String IP_V4_DOMAIN_PATTERN =
    "/^(\\d{1,3})[.](\\d{1,3})[.](\\d{1,3})[.](\\d{1,3})$/";

  private static final String DOMAIN_PATTERN =
    "/^" + ATOM + "(\\." + ATOM + ")*$/";

  private static final String PORT_PATTERN = "/^:(\\d{1,5})$/";

  private static final String ATOM_PATTERN = "/(" + ATOM + ")/";

  private static final String ALPHA_PATTERN = "/^[" + ALPHA_CHARS + "]/";
  
  private static final UrlValidator VALIDATOR = new UrlValidator();

  private UrlValidator() { 
  }
  
  public static UrlValidator get() {
    return VALIDATOR;
  }

  /**
   * <p>Checks if a field has a valid url address.</p>
   *
   * @param value The value validation is being performed on.  A <code>null</code>
   * value is considered invalid.
   * @return true if the url is valid.
   */
  public boolean isValid(String value) {
    if (value == null) {
      return false;
    }

    Perl5Util matchUrlPat = new Perl5Util();
    Perl5Util matchAsciiPat = new Perl5Util();

    if (!matchAsciiPat.match(LEGAL_ASCII_PATTERN, value)) {
      return false;
    }

    // Check the whole url address structure
    if (!matchUrlPat.match(URL_PATTERN, value)) {
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
   * @param scheme The scheme to validate.  A <code>null</code> value is considered
   * invalid.
   * @return true if valid.
   */
  protected boolean isValidScheme(String scheme) {
    if (scheme == null) {
      return false;
    }

    Perl5Util schemeMatcher = new Perl5Util();
    if (!schemeMatcher.match(SCHEME_PATTERN, scheme)) {
      return false;
    }

    return true;
  }

  /**
   * Returns true if the authority is properly formatted.  An authority is the combination
   * of hostname and port.  A <code>null</code> authority value is considered invalid.
   * @param authority Authority value to validate.
   * @return true if authority (hostname and port) is valid.
   */
  protected boolean isValidAuthority(String authority) {
    if (authority == null) {
      return false;
    }

    Perl5Util authorityMatcher = new Perl5Util();
    Perl5Util matchIPV4Pat = new Perl5Util();

    if (!authorityMatcher.match(AUTHORITY_PATTERN, authority)) {
      return false;
    }

    boolean ipV4Address = false;
    boolean hostname = false;
    // check if authority is IP address or hostname
    String hostIP = authorityMatcher.group(PARSE_AUTHORITY_HOST_IP);
    ipV4Address = matchIPV4Pat.match(IP_V4_DOMAIN_PATTERN, hostIP);

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
      Perl5Util domainMatcher = new Perl5Util();
      hostname = domainMatcher.match(DOMAIN_PATTERN, hostIP);
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
      boolean match = true;
      int segCount = 0;
      int segLen = 0;
      Perl5Util atomMatcher = new Perl5Util();

      while (match) {
        match = atomMatcher.match(ATOM_PATTERN, hostIP);
        if (match) {
          domainSegment[segCount] = atomMatcher.group(1);
          segLen = domainSegment[segCount].length() + 1;
          hostIP = (segLen >= hostIP.length()) ? "" 
                                               : hostIP.substring(segLen);
          segCount++;
        }
      }
      String topLevel = domainSegment[segCount - 1];
      if (topLevel.length() < 2 || topLevel.length() > 4) {
        return false;
      }

      // First letter of top level must be a alpha
      Perl5Util alphaMatcher = new Perl5Util();
      if (!alphaMatcher.match(ALPHA_PATTERN, topLevel.substring(0, 1))) {
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
      Perl5Util portMatcher = new Perl5Util();
      if (!portMatcher.match(PORT_PATTERN, port)) {
        return false;
      }
    }

    String extra = authorityMatcher.group(PARSE_AUTHORITY_EXTRA);
    if (!isBlankOrNull(extra)) {
      return false;
    }

    return true;
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
   * Returns true if the path is valid.  A <code>null</code> value is considered invalid.
   * @param path Path value to validate.
   * @return true if path is valid.
   */
  protected boolean isValidPath(String path) {
    if (path == null) {
      return false;
    }

    Perl5Util pathMatcher = new Perl5Util();

    if (!pathMatcher.match(PATH_PATTERN, path)) {
      return false;
    }

    int slash2Count = countToken("//", path);

    int slashCount = countToken("/", path);
    int dot2Count = countToken("..", path);
    if (dot2Count > 0) {
      if ((slashCount - slash2Count - 1) <= dot2Count) {
        return false;
      }
    }

    return true;
  }

  /**
   * Returns true if the query is null or it's a properly formatted query string.
   * @param query Query value to validate.
   * @return true if query is valid.
   */
  protected boolean isValidQuery(String query) {
    if (query == null) {
      return true;
    }

    Perl5Util queryMatcher = new Perl5Util();
    return queryMatcher.match(QUERY_PATTERN, query);
  }

  /**
   * Returns the number of times the token appears in the target.
   * @param token Token value to be counted.
   * @param target Target value to count tokens in.
   * @return the number of tokens.
   */
  protected int countToken(String token, String target) {
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

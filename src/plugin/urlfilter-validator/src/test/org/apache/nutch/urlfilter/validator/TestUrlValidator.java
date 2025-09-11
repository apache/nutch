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
package org.apache.nutch.urlfilter.validator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * JUnit test case which tests 1. that valid urls are not filtered while invalid
 * ones are filtered. 2. that Urls' scheme, authority, path and query are
 * validated.
 * 
 * @author tejasp
 * 
 */

public class TestUrlValidator {

  /**
   * Test method for
   * {@link org.apache.nutch.urlfilter.validator.UrlValidator#filter(java.lang.String)}
   * .
   */
  @Test
  public void testFilter() {
    UrlValidator url_validator = new UrlValidator();
    assertNotNull(url_validator);

    assertNull(url_validator.filter(null),
        "Filtering on a null object should return null");
    assertNull(url_validator.filter("example.com/file[/].html"),
        "Invalid url: example.com/file[/].html");
    assertNull(url_validator.filter("http://www.example.com/space here.html"),
        "Invalid url: http://www.example.com/space here.html");
    assertNull(url_validator.filter("/main.html"),
        "Invalid url: /main.html");
    assertNull(url_validator.filter("www.example.com/main.html"),
        "Invalid url: www.example.com/main.html");
    assertNull(url_validator.filter("ftp:www.example.com/main.html"),
        "Invalid url: ftp:www.example.com/main.html");
    assertNull(
        url_validator.filter("http://999.000.456.32/nutch/trunk/README.txt"),
        "Inalid url: http://999.000.456.32/nutch/trunk/README.txt");
    assertNull(url_validator.filter(" http://www.example.com/ma|in\\toc.html"),
        "Invalid url: http://www.example.com/ma|in\\toc.html");

    assertNotNull(
        url_validator.filter("https://issues.apache.org/jira/NUTCH-1127"),
        "Valid url: https://issues.apache.org/jira/NUTCH-1127");
    assertNotNull(url_validator
                .filter("http://domain.tld/function.cgi?url=http://fonzi.com/&amp;name=Fonzi&amp;mood=happy&amp;coat=leather"),
        "Valid url: http://domain.tld/function.cgi?url=http://fonzi.com/&amp;name=Fonzi&amp;mood=happy&amp;coat=leather");
    assertNotNull(url_validator
                .filter("http://validator.w3.org/feed/check.cgi?url=http%3A%2F%2Ffeeds.feedburner.com%2Fperishablepress"),
        "Valid url: http://validator.w3.org/feed/check.cgi?url=http%3A%2F%2Ffeeds.feedburner.com%2Fperishablepress");
    assertNotNull(url_validator.filter("ftp://alfa.bravo.pi/mike/check/plan.pdf"),
        "Valid url: ftp://alfa.bravo.pi/foo/bar/plan.pdf");

  }
}

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

import org.apache.nutch.urlfilter.validator.UrlValidator;
import junit.framework.TestCase;

/**
 * JUnit test case which tests 
 * 1. that valid urls are not filtered while invalid ones are filtered.
 * 2. that Urls' scheme, authority, path and query are validated.
 * 
 * @author tejasp
 *
 */

public class TestUrlValidator extends TestCase {

  /**
   * Test method for {@link org.apache.nutch.urlfilter.validator.UrlValidator#filter(java.lang.String)}.
   */
  public void testFilter() {
    UrlValidator url_validator = new UrlValidator();
    assertNotNull(url_validator);

    assertNull("Filtering on a null object should return null", url_validator.filter(null));
    assertNull("Invalid url: example.com/file[/].html", url_validator.filter("example.com/file[/].html"));
    assertNull("Invalid url: http://www.example.com/space here.html", url_validator.filter("http://www.example.com/space here.html"));
    assertNull("Invalid url: /main.html", url_validator.filter("/main.html"));
    assertNull("Invalid url: www.example.com/main.html", url_validator.filter("www.example.com/main.html"));
    assertNull("Invalid url: ftp:www.example.com/main.html", url_validator.filter("ftp:www.example.com/main.html"));
    assertNull("Inalid url: http://999.000.456.32/nutch/trunk/README.txt", 
        url_validator.filter("http://999.000.456.32/nutch/trunk/README.txt"));
    assertNull("Invalid url: http://www.example.com/ma|in\\toc.html", url_validator.filter(" http://www.example.com/ma|in\\toc.html"));

    assertNotNull("Valid url: https://issues.apache.org/jira/NUTCH-1127", url_validator.filter("https://issues.apache.org/jira/NUTCH-1127"));
    assertNotNull("Valid url: http://domain.tld/function.cgi?url=http://fonzi.com/&amp;name=Fonzi&amp;mood=happy&amp;coat=leather", 
        url_validator.filter("http://domain.tld/function.cgi?url=http://fonzi.com/&amp;name=Fonzi&amp;mood=happy&amp;coat=leather"));
    assertNotNull("Valid url: http://validator.w3.org/feed/check.cgi?url=http%3A%2F%2Ffeeds.feedburner.com%2Fperishablepress", 
        url_validator.filter("http://validator.w3.org/feed/check.cgi?url=http%3A%2F%2Ffeeds.feedburner.com%2Fperishablepress"));
    assertNotNull("Valid url: ftp://alfa.bravo.pi/foo/bar/plan.pdf", url_validator.filter("ftp://alfa.bravo.pi/mike/check/plan.pdf"));

  }
}

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
package org.apache.nutch.net.protocols;

import java.text.ParseException;
import java.util.Date;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestHttpDateFormat {

  /**
   * Test date as string and epoche milliseconds:
   * 
   * <pre>
   *   $> date --date "Sun, 06 Nov 1994 08:49:37 GMT" '+%s'
   *   784111777
   * </pre>
   */
  private final String dateString = "Sun, 06 Nov 1994 08:49:37 GMT";
  private long dateMillis = 784111777000L;

  @Test
  public void testHttpDateFormat() throws ParseException {

    assertThat(HttpDateFormat.toLong(dateString), is(dateMillis));
    assertThat(HttpDateFormat.toString(dateMillis), is(dateString));
    assertThat(HttpDateFormat.toDate(dateString), is(new Date(dateMillis)));

    String ds2 = "Sun, 6 Nov 1994 08:49:37 GMT";
    assertThat(HttpDateFormat.toLong(ds2), is(dateMillis));
  }

  @Test
  public void testHttpDateFormatException() throws ParseException {
    String ds = "this is not a valid date";
    Exception exception = assertThrows(ParseException.class, () -> {
      HttpDateFormat.toLong(ds);
    });
    String expectedMessage =
        "Text 'this is not a valid date' could not be parsed at index 0";
    String actualMessage = exception.getMessage();
    assertTrue(actualMessage.contains(expectedMessage));

  }

  /**
   * NUTCH-2814 - HttpDateFormat's internal time zone must not change when
   * parsing a date using a different time zone
   */
  @Test
  public void testHttpDateFormatTimeZone() throws ParseException {
    String dateStringPDT = "Mon, 21 Oct 2019 03:18:16 PDT";
    HttpDateFormat.toLong(dateStringPDT); // must not affect internal time zone
    assertEquals(dateString, HttpDateFormat.toString(dateMillis));
  }
}

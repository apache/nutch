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

import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Parse and format HTTP dates in HTTP headers, e.g., used to fill the
 * &quot;If-Modified-Since&quot; request header field.
 * 
 * HTTP dates use Greenwich Mean Time (GMT) as time zone and a date format like:
 * 
 * <pre>
 * Sun, 06 Nov 1994 08:49:37 GMT
 * </pre>
 * 
 * See <a href=
 * "https://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.3.1">sec. 3.3.1
 * in RFC 2616</a> and
 * <a href="https://tools.ietf.org/html/rfc7231#section-7.1.1.1">sec. 7.1.1.1 in
 * RFC 7231</a>.
 */
public class HttpDateFormat {

  public static final DateTimeFormatter FORMAT = DateTimeFormatter
      .ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US)
      .withZone(ZoneId.of(ZoneOffset.UTC.toString()));

  /**
   * Use a less restrictive format for parsing: accept single-digit day-of-month
   * and any timezone
   */
  public static final DateTimeFormatter PARSE_FORMAT = DateTimeFormatter
      .ofPattern("EEE, d MMM yyyy HH:mm:ss z", Locale.US)
      .withZone(ZoneId.of(ZoneOffset.UTC.toString()));

  /**
   * Get the HTTP format of the specified date.
   * @param date a {@link java.util.Date} for conversion
   * @return the String HTTP representation of the date
   */
  public static String toString(Date date) {
    return FORMAT.format(date.toInstant());
  }

  public static String toString(Calendar cal) {
    return FORMAT.format(cal.toInstant());
  }

  public static String toString(long millis) {
    return FORMAT.format(Instant.ofEpochMilli(millis));
  }

  public static ZonedDateTime toZonedDateTime(String dateString) throws ParseException {
    try {
      return PARSE_FORMAT.parse(dateString, ZonedDateTime::from);
    } catch (DateTimeParseException ex) {
      throw new ParseException(ex.getMessage(), 0);
    }
  }

  public static Date toDate(String dateString) throws ParseException {
    return Date.from(toZonedDateTime(dateString).toInstant());
  }

  public static long toLong(String dateString) throws ParseException {
    return toZonedDateTime(dateString).toInstant().toEpochMilli();
  }

  public static void main(String[] args) throws Exception {
    Date now = new Date(System.currentTimeMillis());

    String string = HttpDateFormat.toString(now);

    long time = HttpDateFormat.toLong(string);

    System.out.println(string);
    System.out.println(HttpDateFormat.toString(time));
  }

}

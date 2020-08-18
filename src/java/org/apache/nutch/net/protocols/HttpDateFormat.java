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
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import java.text.ParseException;

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

  protected static SimpleDateFormat format = new SimpleDateFormat(
      "EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);

  protected static TimeZone gmt = TimeZone.getTimeZone("GMT");

  /**
   * HTTP date uses TimeZone GMT
   */
  static {
    format.setTimeZone(gmt);
  }

  /**
   * Get the HTTP format of the specified date.
   */
  public static String toString(Date date) {
    String string;
    synchronized (format) {
      string = format.format(date);
    }
    return string;
  }

  public static String toString(Calendar cal) {
    String string;
    synchronized (format) {
      string = format.format(cal.getTime());
    }
    return string;
  }

  public static String toString(long time) {
    String string;
    synchronized (format) {
      string = format.format(new Date(time));
    }
    return string;
  }

  public static Date toDate(String dateString) throws ParseException {
    Date date;
    synchronized (format) {
      date = format.parse(dateString);
      format.setTimeZone(gmt);
    }
    return date;
  }

  public static long toLong(String dateString) throws ParseException {
    long time;
    synchronized (format) {
      time = format.parse(dateString).getTime();
      format.setTimeZone(gmt);
    }
    return time;
  }

  public static void main(String[] args) throws Exception {
    Date now = new Date(System.currentTimeMillis());

    String string = HttpDateFormat.toString(now);

    long time = HttpDateFormat.toLong(string);

    System.out.println(string);
    System.out.println(HttpDateFormat.toString(time));
  }

}

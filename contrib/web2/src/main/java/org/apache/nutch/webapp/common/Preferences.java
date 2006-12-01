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
package org.apache.nutch.webapp.common;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Locale;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Preferences represents (extendable) configuration object that is persistable
 * into user browser (as cookie).
 *
 * Cookie is in format key-1<KEYVALSEPARATOR>value-1<VALVALSEPARATOR>key-2<KEYVALSEPARATOR>value-2<VALVALSEPARATOR>key-n<KEYVALSEPARATOR>value-n
 */
public class Preferences extends HashMap {

  private static final long serialVersionUID = 1;

  public static final String KEY_LOCALE = "L";

  public static final String KEY_RESULTS_PER_PAGE = "R";

  public static final String KEY_HITS_PER_DUP = "S";

  public static final String KEY_DUP_FIELD = "D";

  static final String DEFAULTKEYVALSEPARATOR = "_";

  static final String DEFAULTVALVALSEPARATOR = "-";

  /**
   *  Name of web ui cookie that stores users cutomized user preferences.
   */
  public static final String COOKIE_NAME = "NUTCH";

  /**
   *  Default preferences, used for all who have not customized.
   */
  static final Preferences DEFAULTS = new Preferences();

  static {
    // results per page
    DEFAULTS.put(KEY_RESULTS_PER_PAGE, "10");

    // dup field
    DEFAULTS.put(KEY_DUP_FIELD, "site");
  }

  /**
   * @return locale of user (from preferences if set, or from request)
   */
  public Locale getLocale(HttpServletRequest request) {
    if (containsKey(KEY_LOCALE)) {
      return new Locale((String) get(KEY_LOCALE));
    } else {
      return request.getLocale();
    }
  }

  /**
   * Persist Preferences as cookie.
   * @param request
   * @param response
   * @param prefs   preferences object to persist
   */
  public static void setPreferencesCookie(HttpServletRequest request,
      HttpServletResponse response, Preferences prefs) {
    if (DEFAULTS.equals(prefs)) {
      removeCookie(response);
    } else {
      setPreferencesCookie(response, prefs);
    }
  }

  private static void setPreferencesCookie(HttpServletResponse response,
      Preferences prefs) {
    Cookie prefscookie = new Cookie(COOKIE_NAME, prefs.toString());
    prefscookie.setMaxAge(Integer.MAX_VALUE);
    response.addCookie(prefscookie);
  }

  /**
   * Remove cookie from browser
   *
   * @param response
   */
  public static void removeCookie(HttpServletResponse response) {
    Cookie prefscookie = new Cookie(COOKIE_NAME, "");
    prefscookie.setMaxAge(-1);
    response.addCookie(prefscookie);
  }

  /**
   * Parse Preferences from cookie.
   *
   * @param request
   */
  public static Preferences parseCookie(HttpServletRequest request) {
    // find right cookie
    Cookie[] c = request.getCookies();

    if (c != null) {
      for (int i = 0; i < c.length; i++) {
        if (COOKIE_NAME.equals(c[i].getName())) {
          return Preferences.parse(c[i].getValue());
        }
      }
    }
    return DEFAULTS;
  }

  /**
   * Parse a String into Preferences object using default separators
   * @param data
   * @return
   */
  public static Preferences parse(String data) {
    return parse(data,DEFAULTVALVALSEPARATOR, DEFAULTKEYVALSEPARATOR);
  }

  /**
   * Parse a String into a Preferences object 
   * @param data String to parse
   * @param valueValueSeparator delimiter between keyValue pairs
   * @param keyValueSeparator delimiter between key & value
   * @return parsed Preferences
   */
  public static Preferences parse(String data, String valueValueSeparator, String keyValueSeparator) {
    Preferences p = new Preferences();
    p.putAll(DEFAULTS);
    String[] dataitems = data.split(valueValueSeparator);
    for (int i = 0; i < dataitems.length; i++) {
      String keyvalue[] = dataitems[i].split(keyValueSeparator);
      if (keyvalue.length == 2) {
        try {
          p.put(keyvalue[0], URLDecoder.decode((String) keyvalue[1], "UTF-8"));
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
        }
      }
    }
    return p;
  }

  /**
   * Return int value or default if non existing.
   * @param name
   * @param defaultVal
   * @return
   */
  public int getInt(String name, int defaultVal) {
    try {
      return get(name) == null ? defaultVal : Integer
          .parseInt((String) get(name));
    } catch (Exception e) {
      return defaultVal;
    }
  }

  /**
   * Return String value or default if non existing.
   * @param name
   * @param defaultVal
   * @return
   */
  public String getString(String name, String defaultVal) {
    return get(name) == null ? defaultVal : (String) get(name);
  }

  public String toString() {
    StringBuffer txt = new StringBuffer();
    Object[] keys = keySet().toArray();

    for (int i = 0; i < keys.length; i++) {
      try {
        txt.append(keys[i].toString()).append(DEFAULTKEYVALSEPARATOR).append(
            URLEncoder.encode((String) get(keys[i]), "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
      if (i < keys.length - 1) {
        txt.append(DEFAULTVALVALSEPARATOR);
      }
    }

    return txt.toString();
  }

  /**
   * Get (cached) Preferences instance from request (first from request
   * attribute then from cookie).
   *
   * @param request
   * @return Preferences object from request or null if not available
   */
  public static Preferences getPreferences(final HttpServletRequest request) {
    Preferences prefs = (Preferences) request.getAttribute(Preferences.class
        .getName());
    // processing locale
    if (prefs == null) {
      prefs = parseCookie(request);
      request.setAttribute(Preferences.class.getName(), prefs);
    }
    return prefs;
  }

}

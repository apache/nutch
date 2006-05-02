/*
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.nutch.html.Entities;

/**
 * Preferences represents (extendable) configuration object that is persistable
 * into user browser (as cookie)
 * 
 * cookie is in format key-1=value-1,key-2=value-2,key-n=value-n
 */
public class Preferences extends HashMap {

  private static final long serialVersionUID = 1;

  public static final String KEY_LOCALE = "L";

  public static final String KEY_RESULTS_PER_PAGE = "R";

  public static final String KEY_HITS_PER_DUP = "S";

  public static final String KEY_DUP_FIELD = "D";
  
  static final String KEYVALSEPARATOR="_";
  static final String VALVALSEPARATOR="-";

  // Name of web ui cookie that stores users cutomized user preferences
  public static String COOKIE_NAME = "NUTCH";

  // default preferences, used for all who have not customized
  static Preferences defaults = new Preferences();

  static {
    // results per page
    defaults.put(KEY_RESULTS_PER_PAGE, "10");

    // dup field
    defaults.put(KEY_DUP_FIELD, "site");
  }

  /**
   * 
   * @return locale of user (from preferences if set, or from request)
   */
  public Locale getLocale(HttpServletRequest request) {
    if (containsKey(KEY_LOCALE))
      return new Locale((String) get(KEY_LOCALE));
    else
      return request.getLocale();
  }

  public static void setPreferencesCookie(HttpServletRequest request,
      HttpServletResponse response, Preferences prefs) {
    if (defaults.equals(prefs)) {
      System.out.println("default qeuals prefs, removing");
      removeCookie(response);
    } else {
      System.out.println("setting preferences to cookie");
      setPreferencesCookie(response, prefs);
    }
  }

  private static void setPreferencesCookie(HttpServletResponse response,
      Preferences prefs) {
    Cookie prefscookie = new Cookie(COOKIE_NAME, prefs.toString());
    prefscookie.setMaxAge(Integer.MAX_VALUE);
    response.addCookie(prefscookie);
  }

  public static void removeCookie(HttpServletResponse response) {
    Cookie prefscookie = new Cookie(COOKIE_NAME, "");
    prefscookie.setMaxAge(-1);
    response.addCookie(prefscookie);
  }

  /**
   * Parse Preferences from cookie
   * 
   * @param request
   */
  public static Preferences parseCookie(HttpServletRequest request) {
    // find right cookie
    Cookie c[] = request.getCookies();

    if (c != null) {
      for (int i = 0; i < c.length; i++) {
        if (COOKIE_NAME.equals(c[i].getName())) {
          return Preferences.parse(c[i].getValue());
        }
      }
    }
    return defaults;
  }

  public static Preferences parse(String data) {
    return parse(data,VALVALSEPARATOR, KEYVALSEPARATOR);
  }

  public static Preferences parse(String data, String valueValueSeparator, String keyValueSeparator) {

    System.out.println("data:" + data);
    Preferences p = new Preferences();
    p.putAll(defaults);
    String[] dataitems = data.split(valueValueSeparator);
    System.out.println(dataitems.length + " dataitems submitted");
    for (int i = 0; i < dataitems.length; i++) {
      String keyvalue[] = dataitems[i].split(keyValueSeparator);
      if (keyvalue.length == 2) {
        try {
          System.out.println("adding:" +  keyvalue[0] + "=" + keyvalue[1]);
          p.put(keyvalue[0], URLDecoder.decode((String)keyvalue[1],"UTF-8"));
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
        }
      }
    }
    return p;
  }
  
  public int getInt(String name, int defaultVal) {
    try {
      return get(name) == null ? defaultVal : Integer
          .parseInt((String) get(name));
    } catch (Exception e) {
      return defaultVal;
    }
  }

  /**
   * return value or default if non existing
   * 
   */
  public String getString(String name, String defaultVal) {
    return get(name) == null ? defaultVal : (String) get(name);
  }

  public String toString() {
    StringBuffer txt = new StringBuffer();
    Object[] keys = keySet().toArray();

    for (int i = 0; i < keys.length; i++) {
      try {
        txt.append(keys[i].toString()).append(KEYVALSEPARATOR).append(URLEncoder.encode((String)get(keys[i]),"UTF-8"));
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
      if (i < keys.length - 1)
        txt.append(VALVALSEPARATOR);
    }

    System.out.println("toString():" + txt.toString());
    
    return txt.toString();
  }

  public static Preferences getPreferences(HttpServletRequest request) {
    Preferences prefs = (Preferences) request.getAttribute(Preferences.class
        .getName());
    // processing locale
    if (prefs == null) {
      prefs = Preferences.parseCookie(request);
      request.setAttribute(Preferences.class.getName(), prefs);
    }
    return prefs;
  }

}

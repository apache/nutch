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
package org.apache.nutch.searcher.response;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;

/**
 * A set of utility methods for getting request paramters.
 */
public class RequestUtils {

  public static boolean parameterExists(HttpServletRequest request, String param) {
    String value = request.getParameter(param);
    return value != null;
  }

  public static Integer getIntegerParameter(HttpServletRequest request,
    String param) {
    if (parameterExists(request, param)) {
      String value = request.getParameter(param);
      if (StringUtils.isNotBlank(value) && StringUtils.isNumeric(value)) {
        return new Integer(value);
      }
    }
    return null;
  }

  public static Integer getIntegerParameter(HttpServletRequest request,
    String param, Integer def) {
    Integer value = getIntegerParameter(request, param);
    return (value == null) ? def : value;
  }

  public static String getStringParameter(HttpServletRequest request,
    String param) {
    if (parameterExists(request, param)) {
      return request.getParameter(param);
    }
    return null;
  }

  public static String getStringParameter(HttpServletRequest request,
    String param, String def) {
    String value = getStringParameter(request, param);
    return (value == null) ? def : value;
  }

  public static Boolean getBooleanParameter(HttpServletRequest request,
    String param) {
    if (parameterExists(request, param)) {
      String value = request.getParameter(param);
      if (StringUtils.isNotBlank(value)
        && (StringUtils.equals(param, "1")
          || StringUtils.equalsIgnoreCase(param, "true") || StringUtils.equalsIgnoreCase(
          param, "yes"))) {
        return true;
      }
    }
    return false;
  }

  public static Boolean getBooleanParameter(HttpServletRequest request,
    String param, Boolean def) {
    if (parameterExists(request, param)) {
      String value = request.getParameter(param);
      return (StringUtils.isNotBlank(value) && (StringUtils.equals(param, "1")
        || StringUtils.equalsIgnoreCase(param, "true") || StringUtils.equalsIgnoreCase(
        param, "yes")));
    }
    return def;
  }
}

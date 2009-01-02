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

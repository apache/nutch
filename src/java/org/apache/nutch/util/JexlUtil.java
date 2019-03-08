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
package org.apache.nutch.util;

import java.lang.invoke.MethodHandles;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.lang.time.DateUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for handling JEXL expressions
 */
public class JexlUtil {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /** Supported format for date parsing yyyy-MM-ddTHH:mm:ssZ */
  private static final Pattern DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z");

  /**
   * Parses the given expression to a JEXL expression. This supports
   * date parsing.
   *
   * @param expr string JEXL expression
   * @return parsed JEXL expression or null in case of parse error
   */
  public static Expression parseExpression(String expr) {
    if (expr == null) return null;
    
    try {
      // Translate any date object into a long. Dates must be in the DATE_PATTERN
      // format. For example: 2016-03-20T00:00:00Z
      Matcher matcher = DATE_PATTERN.matcher(expr);

      if (matcher.find()) {
        String date = matcher.group();
        
        // parse the matched substring and get the epoch
        Date parsedDate = DateUtils.parseDateStrictly(date, new String[] {"yyyy-MM-dd'T'HH:mm:ss'Z'"});
        long time = parsedDate.getTime();
        
        // replace the original string date with the numeric value
        expr = expr.replace(date, Long.toString(time));
      }

      JexlEngine jexl = new JexlEngine();

      jexl.setSilent(true);
      jexl.setStrict(true);

      return jexl.createExpression(expr);
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
    
    return null;
  }
}

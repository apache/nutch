/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.parse;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.util.TableUtil;

public class ParseStatusUtils {

  public static ParseStatus STATUS_SUCCESS = new ParseStatus();
  public static final HashMap<Short,String> minorCodes = new HashMap<Short,String>();

  static {
    STATUS_SUCCESS.setMajorCode(ParseStatusCodes.SUCCESS);
    minorCodes.put(ParseStatusCodes.SUCCESS_OK, "ok");
    minorCodes.put(ParseStatusCodes.SUCCESS_REDIRECT, "redirect");
    minorCodes.put(ParseStatusCodes.FAILED_EXCEPTION, "exception");
    minorCodes.put(ParseStatusCodes.FAILED_INVALID_FORMAT, "invalid_format");
    minorCodes.put(ParseStatusCodes.FAILED_MISSING_CONTENT, "missing_content");
    minorCodes.put(ParseStatusCodes.FAILED_MISSING_PARTS, "missing_parts");
    minorCodes.put(ParseStatusCodes.FAILED_TRUNCATED, "truncated");
  }

  public static boolean isSuccess(ParseStatus status) {
    if (status == null) {
      return false;
    }
    return status.getMajorCode() == ParseStatusCodes.SUCCESS;
  }

  /** A convenience method. Return a String representation of the first
   * argument, or null.
   */
  public static String getMessage(ParseStatus status) {
    GenericArray<Utf8> args = status.getArgs();
    if (args != null && args.size() > 0) {
      return TableUtil.toString(args.iterator().next());
    }
    return null;
  }

  public static String getArg(ParseStatus status, int n) {
    GenericArray<Utf8> args = status.getArgs();
    if (args == null) {
      return null;
    }
    int i = 0;
    for (Utf8 arg : args) {
      if (i == n) {
        return TableUtil.toString(arg);
      }
      i++;
    }
    return null;
  }

  public static Parse getEmptyParse(Exception e, Configuration conf) {
    ParseStatus status = new ParseStatus();
    status.setMajorCode(ParseStatusCodes.FAILED);
    status.setMinorCode(ParseStatusCodes.FAILED_EXCEPTION);
    status.addToArgs(new Utf8(e.toString()));

    return new Parse("", "", new Outlink[0], status);
  }

  public static Parse getEmptyParse(int minorCode, String message, Configuration conf) {
    ParseStatus status = new ParseStatus();
    status.setMajorCode(ParseStatusCodes.FAILED);
    status.setMinorCode(minorCode);
    status.addToArgs(new Utf8(message));

    return new Parse("", "", new Outlink[0], status);
  }
  
  public static String toString(ParseStatus status) {
    if (status == null) {
      return "(null)";
    }
    StringBuilder sb = new StringBuilder();
    sb.append(ParseStatusCodes.majorCodes[status.getMajorCode()] +
        "/" + minorCodes.get((short)status.getMinorCode()));
    sb.append(" (" + status.getMajorCode() + "/" + status.getMinorCode() + ")");
    sb.append(", args=[");
    GenericArray<Utf8> args = status.getArgs();
    if (args != null) {
      int i = 0;
      Iterator<Utf8> it = args.iterator();
      while (it.hasNext()) {
        if (i > 0) sb.append(',');
        sb.append(it.next());
        i++;
      }
    }
    sb.append("]");
    return sb.toString();
  }
}

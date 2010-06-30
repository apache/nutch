package org.apache.nutch.parse;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.util.TableUtil;

public class ParseStatusUtils {

  public static ParseStatus STATUS_SUCCESS = new ParseStatus();

  static {
    STATUS_SUCCESS.setMajorCode(ParseStatusCodes.SUCCESS);
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
}

/**
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

/**
 * A collection of String processing utility methods. 
 */
public class StringUtil {

  /**
   * Returns a copy of <code>s</code> padded with trailing spaces so
   * that it's length is <code>length</code>.  Strings already
   * <code>length</code> characters long or longer are not altered.
   */
  public static String rightPad(String s, int length) {
    StringBuffer sb= new StringBuffer(s);
    for (int i= length - s.length(); i > 0; i--) 
      sb.append(" ");
    return sb.toString();
  }

  /**
   * Returns a copy of <code>s</code> padded with leading spaces so
   * that it's length is <code>length</code>.  Strings already
   * <code>length</code> characters long or longer are not altered.
   */
  public static String leftPad(String s, int length) {
    StringBuffer sb= new StringBuffer();
    for (int i= length - s.length(); i > 0; i--) 
      sb.append(" ");
    sb.append(s);
    return sb.toString();
  }


  private static final char[] HEX_DIGITS =
  {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

  /**
   * Convenience call for {@link #toHexString(byte[], String, int)}, where
   * <code>sep = null; lineLen = Integer.MAX_VALUE</code>.
   * @param buf
   */
  public static String toHexString(byte[] buf) {
    return toHexString(buf, null, Integer.MAX_VALUE);
  }

  /**
   * Get a text representation of a byte[] as hexadecimal String, where each
   * pair of hexadecimal digits corresponds to consecutive bytes in the array.
   * @param buf input data
   * @param sep separate every pair of hexadecimal digits with this separator, or
   * null if no separation is needed.
   * @param lineLen break the output String into lines containing output for lineLen
   * bytes.
   */
  public static String toHexString(byte[] buf, String sep, int lineLen) {
    if (buf == null) return null;
    if (lineLen <= 0) lineLen = Integer.MAX_VALUE;
    StringBuffer res = new StringBuffer(buf.length * 2);
    for (int i = 0; i < buf.length; i++) {
      int b = buf[i];
      res.append(HEX_DIGITS[(b >> 4) & 0xf]);
      res.append(HEX_DIGITS[b & 0xf]);
      if (i > 0 && (i % lineLen) == 0) res.append('\n');
      else if (sep != null && i < lineLen - 1) res.append(sep); 
    }
    return res.toString();
  }
  
  /**
   * Convert a String containing consecutive (no inside whitespace) hexadecimal
   * digits into a corresponding byte array. If the number of digits is not even,
   * a '0' will be appended in the front of the String prior to conversion.
   * Leading and trailing whitespace is ignored.
   * @param text input text
   * @return converted byte array, or null if unable to convert
   */
  public static byte[] fromHexString(String text) {
    text = text.trim();
    if (text.length() % 2 != 0) text = "0" + text;
    int resLen = text.length() / 2;
    int loNibble, hiNibble;
    byte[] res = new byte[resLen];
    for (int i = 0; i < resLen; i++) {
      int j = i << 1;
      hiNibble = charToNibble(text.charAt(j));
      loNibble = charToNibble(text.charAt(j + 1));
      if (loNibble == -1 || hiNibble == -1) return null;
      res[i] = (byte)(hiNibble << 4 | loNibble);
    }
    return res;
  }
  
  private static final int charToNibble(char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    } else if (c >= 'a' && c <= 'f') {
      return 0xa + (c - 'a');
    } else if (c >= 'A' && c <= 'F') {
      return 0xA + (c - 'A');
    } else {
      return -1;
    }
  }

  /**
   * Checks if a string is empty (ie is null or empty).
   */
  public static boolean isEmpty(String str) {
    return (str == null) || (str.equals(""));
  }

  public static void main(String[] args) {
    if (args.length != 1)
      System.out.println("Usage: StringUtil <encoding name>");
    else 
      System.out.println(args[0] + " is resolved to " +
                         EncodingDetector.resolveEncodingAlias(args[0]));
  }
}

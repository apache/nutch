/**
 * Copyright 2005 The Apache Software Foundation
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

package org.apache.nutch.util;

import java.util.HashMap;
import java.nio.charset.Charset;

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

  /**
   * Parse the character encoding from the specified content type header.
   * If the content type is null, or there is no explicit character encoding,
   * <code>null</code> is returned.
   * <br />
   * This method was copy from org.apache.catalina.util.RequestUtil 
   * is licensed under the Apache License, Version 2.0 (the "License").
   *
   * @param contentType a content type header
   */
  public static String parseCharacterEncoding(String contentType) {
    if (contentType == null)
      return (null);
    int start = contentType.indexOf("charset=");
    if (start < 0)
      return (null);
    String encoding = contentType.substring(start + 8);
    int end = encoding.indexOf(';');
    if (end >= 0)
      encoding = encoding.substring(0, end);
    encoding = encoding.trim();
    if ((encoding.length() > 2) && (encoding.startsWith("\""))
      && (encoding.endsWith("\"")))
      encoding = encoding.substring(1, encoding.length() - 1);
    return (encoding.trim());

  }

  private static HashMap encodingAliases = new HashMap();

  /** 
   * the following map is not an alias mapping table, but
   * maps character encodings which are often used in mislabelled
   * documents to their correct encodings. For instance,
   * there are a lot of documents labelled 'ISO-8859-1' which contain
   * characters not covered by ISO-8859-1 but covered by windows-1252. 
   * Because windows-1252 is a superset of ISO-8859-1 (sharing code points
   * for the common part), it's better to treat ISO-8859-1 as
   * synonymous with windows-1252 than to reject, as invalid, documents
   * labelled as ISO-8859-1 that have characters outside ISO-8859-1.
   */
  static {
    encodingAliases.put("ISO-8859-1", "windows-1252"); 
    encodingAliases.put("EUC-KR", "x-windows-949"); 
    encodingAliases.put("x-EUC-CN", "GB18030"); 
    encodingAliases.put("GBK", "GB18030"); 
 // encodingAliases.put("Big5", "Big5HKSCS"); 
 // encodingAliases.put("TIS620", "Cp874"); 
 // encodingAliases.put("ISO-8859-11", "Cp874"); 

  }

  public static String resolveEncodingAlias(String encoding) {
    if (!Charset.isSupported(encoding))
      return null;
    String canonicalName = new String(Charset.forName(encoding).name());
    return encodingAliases.containsKey(canonicalName) ? 
           (String) encodingAliases.get(canonicalName) : canonicalName; 
  }

  public static void main(String[] args) {
    if (args.length != 1)
      System.out.println("Usage: StringUtil <encoding name>");
    else 
      System.out.println(args[0] + " is resolved to " +
                         resolveEncodingAlias(args[0])); 
  }
}

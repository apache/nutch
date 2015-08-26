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

package org.apache.nutch.indexwriter.cloudsearch;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;

public class CloudSearchUtils {

  private static MessageDigest digester;

  static {
    try {
      digester = MessageDigest.getInstance("SHA-512");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  /** Returns a normalised doc ID based on the URL of a document **/
  public static String getID(String url) {

    // the document needs an ID
    // @see
    // http://docs.aws.amazon.com/cloudsearch/latest/developerguide/preparing-data.html#creating-document-batches
    // A unique ID for the document. A document ID can contain any
    // letter or number and the following characters: _ - = # ; : / ? @
    // &. Document IDs must be at least 1 and no more than 128
    // characters long.
    byte[] dig = digester.digest(url.getBytes(StandardCharsets.UTF_8));
    String ID = Hex.encodeHexString(dig);
    // is that even possible?
    if (ID.length() > 128) {
      throw new RuntimeException("ID larger than max 128 chars");
    }
    return ID;
  }

  public static String stripNonCharCodepoints(String input) {
    StringBuilder retval = new StringBuilder();
    char ch;

    for (int i = 0; i < input.length(); i++) {
      ch = input.charAt(i);

      // Keep only characters that are legal for CloudSearch
      if ((ch == 0x9 || ch == 0xa || ch == 0xd)
          || (ch >= 0x20 && ch <= 0xFFFD)) {
        retval.append(ch);
      }
    }

    return retval.toString();
  }
}

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

package org.apache.nutch.crawl;

import org.apache.hadoop.io.MD5Hash;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.protocol.Content;

/**
 * Implementation of a page signature. It calculates an MD5 hash of the textual
 * content of a page. In case there is no content, it calculates a hash from the
 * page's URL.
 */
public class TextMD5Signature extends Signature {

  Signature fallback = new MD5Signature();

  public byte[] calculate(Content content, Parse parse) {
    String text = parse.getText();

    if (text == null || text.length() == 0) {
      return fallback.calculate(content, parse);
    }

    return MD5Hash.digest(text).getDigest();
  }
}

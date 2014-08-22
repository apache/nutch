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

import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.io.MD5Hash;
import org.apache.nutch.storage.WebPage;

/**
 * Default implementation of a page signature. It calculates an MD5 hash of the
 * textual content of a page. In case there is no text, it calculates a hash
 * from the page's fetched content.
 */
public class TextMD5Signature extends Signature {

  private final static Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.TEXT);
  }

  Signature fallback = new MD5Signature();

  @Override
  public byte[] calculate(WebPage page) {
    CharSequence text = page.getText();

    if (text == null || text.length() == 0) {
      return fallback.calculate(page);
    }

    return MD5Hash.digest(text.toString()).getDigest();
  }

  @Override
  public Collection<WebPage.Field> getFields() {
    Collection<WebPage.Field> fields = new HashSet<WebPage.Field>(FIELDS);
    fields.addAll(fallback.getFields());
    return fields;
  }
}

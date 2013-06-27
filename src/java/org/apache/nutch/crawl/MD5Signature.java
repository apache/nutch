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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.MD5Hash;
import org.apache.nutch.storage.WebPage;

/**
 * Default implementation of a page signature. It calculates an MD5 hash
 * of the raw binary content of a page. In case there is no content, it
 * calculates a hash from the page's URL.
 *
 * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
 */
public class MD5Signature extends Signature {

  private final static Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.CONTENT);
  }

  @Override
  public byte[] calculate(WebPage page) {
    ByteBuffer buf = page.getContent();
    byte[] data;
    int of;
    int cb;
    if (buf == null) {
      Utf8 baseUrl = page.getBaseUrl();
      if (baseUrl == null) {
        data = null;
        of = 0;
        cb = 0;
      }
      else {
        data = baseUrl.getBytes();
        of = 0;
        cb = baseUrl.getLength();
      }
    } else {
      data = buf.array();
      of = buf.arrayOffset() + buf.position();
      cb = buf.remaining();
    }

    return MD5Hash.digest(data, of, cb).getDigest();
  }

  @Override
  public Collection<WebPage.Field> getFields() {
    return FIELDS;
  }
}

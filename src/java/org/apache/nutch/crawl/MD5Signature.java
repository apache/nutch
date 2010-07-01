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
 * Default implementation of a page signature. It calculates an MD5 hash
 * of the raw binary content of a page. In case there is no content, it
 * calculates a hash from the page's URL.
 * 
 * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
 */
public class MD5Signature extends Signature {

  public byte[] calculate(Content content, Parse parse) {
    byte[] data = content.getContent();
    if (data == null) data = content.getUrl().getBytes();
    return MD5Hash.digest(data).getDigest();
  }
}

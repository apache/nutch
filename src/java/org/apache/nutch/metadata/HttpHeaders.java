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
package org.apache.nutch.metadata;

import org.apache.hadoop.io.Text;

/**
 * A collection of HTTP header names.
 * 
 * @see <a href="http://rfc-ref.org/RFC-TEXTS/2616/">Hypertext Transfer Protocol
 *      -- HTTP/1.1 (RFC 2616)</a>
 */
public interface HttpHeaders {

  public static final String TRANSFER_ENCODING = "Transfer-Encoding";

  public static final String CONTENT_ENCODING = "Content-Encoding";

  public static final String CONTENT_LANGUAGE = "Content-Language";

  public static final String CONTENT_LENGTH = "Content-Length";

  public static final String CONTENT_LOCATION = "Content-Location";

  public static final String CONTENT_DISPOSITION = "Content-Disposition";

  public static final String CONTENT_MD5 = "Content-MD5";

  public static final String CONTENT_TYPE = "Content-Type";

  public static final Text WRITABLE_CONTENT_TYPE = new Text(CONTENT_TYPE);

  public static final String LAST_MODIFIED = "Last-Modified";

  public static final String LOCATION = "Location";

}

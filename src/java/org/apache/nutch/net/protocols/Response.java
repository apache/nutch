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

package org.apache.nutch.net.protocols;

import java.net.URL;


/** A response inteface.  Makes all protocols model HTTP. */

public interface Response {

  /** Returns the URL used to retrieve this response. */
  public URL getUrl();

  /** Returns the response code. */
  public int getCode();

  /** Returns the value of a named header. */
  public String getHeader(String name);

  /** Returns the full content of the response. */
  public byte[] getContent();

  /** 
   * Returns the compressed version of the content if the server
   * transmitted a compressed version, or <code>null</code>
   * otherwise. 
   */
  public byte[] getCompressedContent();

  /**
   * Returns the number of 100/Continue headers encountered 
   */
  public int getNumContinues();

}

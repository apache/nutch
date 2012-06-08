/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.protocol;

public interface ProtocolStatusCodes {

  /** Content was retrieved without errors. */
  public static final int SUCCESS              = 1;
  /** Content was not retrieved. Any further errors may be indicated in args. */
  public static final int FAILED               = 2;

  /** This protocol was not found.  Application may attempt to retry later. */
  public static final int PROTO_NOT_FOUND      = 10;
  /** Resource is gone. */
  public static final int GONE                 = 11;
  /** Resource has moved permanently. New url should be found in args. */
  public static final int MOVED                = 12;
  /** Resource has moved temporarily. New url should be found in args. */
  public static final int TEMP_MOVED           = 13;
  /** Resource was not found. */
  public static final int NOTFOUND             = 14;
  /** Temporary failure. Application may retry immediately. */
  public static final int RETRY                = 15;
  /** Unspecified exception occured. Further information may be provided in args. */
  public static final int EXCEPTION            = 16;
  /** Access denied - authorization required, but missing/incorrect. */
  public static final int ACCESS_DENIED        = 17;
  /** Access denied by robots.txt rules. */
  public static final int ROBOTS_DENIED        = 18;
  /** Too many redirects. */
  public static final int REDIR_EXCEEDED       = 19;
  /** Not fetching. */
  public static final int NOTFETCHING          = 20;
  /** Unchanged since the last fetch. */
  public static final int NOTMODIFIED          = 21;
  /** Request was refused by protocol plugins, because it would block.
   * The expected number of milliseconds to wait before retry may be provided
   * in args. */
  public static final int WOULDBLOCK           = 22;
  /** Thread was blocked http.max.delays times during fetching. */
  public static final int BLOCKED              = 23;
}

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

import java.net.URL;
import java.util.Iterator;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.util.TableUtil;

public class ProtocolStatusUtils implements ProtocolStatusCodes {
  // Useful static instances for status codes that don't usually require any
  // additional arguments.
  public static final ProtocolStatus STATUS_SUCCESS = makeStatus(SUCCESS);
  public static final ProtocolStatus STATUS_FAILED = makeStatus(FAILED);
  public static final ProtocolStatus STATUS_GONE = makeStatus(GONE);
  public static final ProtocolStatus STATUS_NOTFOUND = makeStatus(NOTFOUND);
  public static final ProtocolStatus STATUS_RETRY = makeStatus(RETRY);
  public static final ProtocolStatus STATUS_ROBOTS_DENIED = makeStatus(ROBOTS_DENIED);
  public static final ProtocolStatus STATUS_REDIR_EXCEEDED = makeStatus(REDIR_EXCEEDED);
  public static final ProtocolStatus STATUS_NOTFETCHING = makeStatus(NOTFETCHING);
  public static final ProtocolStatus STATUS_NOTMODIFIED = makeStatus(NOTMODIFIED);
  public static final ProtocolStatus STATUS_WOULDBLOCK = makeStatus(WOULDBLOCK);
  public static final ProtocolStatus STATUS_BLOCKED = makeStatus(BLOCKED);

  public static String getName(int code) {
    if (code == SUCCESS)
      return "SUCCESS";
    else if (code == FAILED)
      return "FAILED";
    else if (code == PROTO_NOT_FOUND)
      return "PROTO_NOT_FOUND";
    else if (code == GONE)
      return "GONE";
    else if (code == MOVED)
      return "MOVED";
    else if (code == TEMP_MOVED)
      return "TEMP_MOVED";
    else if (code == NOTFOUND)
      return "NOTFOUND";
    else if (code == RETRY)
      return "RETRY";
    else if (code == EXCEPTION)
      return "EXCEPTION";
    else if (code == ACCESS_DENIED)
      return "ACCESS_DENIED";
    else if (code == ROBOTS_DENIED)
      return "ROBOTS_DENIED";
    else if (code == REDIR_EXCEEDED)
      return "REDIR_EXCEEDED";
    else if (code == NOTFETCHING)
      return "NOTFETCHING";
    else if (code == NOTMODIFIED)
      return "NOTMODIFIED";
    else if (code == WOULDBLOCK)
      return "WOULDBLOCK";
    else if (code == BLOCKED)
      return "BLOCKED";
    return "UNKNOWN_CODE_" + code;
  }

  public static ProtocolStatus makeStatus(int code) {
    ProtocolStatus pstatus = new ProtocolStatus();
    pstatus.setCode(code);
    pstatus.setLastModified(0);
    return pstatus;
  }

  public static ProtocolStatus makeStatus(int code, String message) {
    ProtocolStatus pstatus = makeStatus(code);
    pstatus.addToArgs(new Utf8(message));
    return pstatus;
  }

  public static ProtocolStatus makeStatus(int code, URL url) {
    return makeStatus(code, url.toString());
  }

  public static String getMessage(ProtocolStatus pstatus) {
    GenericArray<Utf8> args = pstatus.getArgs();
    if (args == null || args.size() == 0) {
      return null;
    }
    return TableUtil.toString(args.iterator().next());
  }
  
  public static String toString(ProtocolStatus status) {
    if (status == null) {
      return "(null)";
    }
    StringBuilder sb = new StringBuilder();
    sb.append(getName(status.getCode()));
    sb.append(", args=[");
    GenericArray<Utf8> args = status.getArgs();
    if (args != null) {
      int i = 0;
      Iterator<Utf8> it = args.iterator();
      while (it.hasNext()) {
        if (i > 0) sb.append(',');
        sb.append(it.next());
        i++;
      }
    }
    sb.append("]");
    return sb.toString();
  }
}

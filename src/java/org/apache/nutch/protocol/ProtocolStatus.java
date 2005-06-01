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

package org.apache.nutch.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.nutch.io.Writable;
import org.apache.nutch.io.WritableUtils;
import org.apache.nutch.parse.ParseStatus;

/**
 * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
 */
public class ProtocolStatus implements Writable {
  
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
  public static final int NOT_FOUND            = 14;
  /** Temporary failure. Application may retry immediately. */
  public static final int RETRY                = 15;
  /** Unspecified exception occured. Further information may be provided in args. */
  public static final int EXCEPTION            = 16;
  /** Access denied - authorization required, but missing/incorrect. */
  public static final int ACCESS_DENIED        = 17;
  /** Access denied by robots.txt rules. */
  public static final int ROBOTS_DENIED        = 18;
  /** Too many redirects. */
  public static final int REDIR_EXCEED         = 19;
  /** Not fetching. */
  public static final int NOTFETCHING          = 20;
  /** Unchanged since the last fetch. */
  public static final int NOTMODIFIED          = 21;
  
  
  public static final ProtocolStatus STATUS_SUCCESS = new ProtocolStatus(SUCCESS);
  public static final ProtocolStatus STATUS_NOTFETCHING = new ProtocolStatus(NOTFETCHING);
  public static final ProtocolStatus STATUS_FAILED = new ProtocolStatus(FAILED);
  public static final ProtocolStatus STATUS_NOTMODIFIED = new ProtocolStatus(NOTMODIFIED);
  
  private int code;
  private String[] args;
  
  protected ProtocolStatus() {
    
  }

  public ProtocolStatus(int code, String[] args) {
    this.code = code;
    this.args = args;
  }
  
  public ProtocolStatus(int code) {
    this(code, null);
  }
  
  public ProtocolStatus(int code, Object message) {
    this.code = code;
    this.args = new String[]{String.valueOf(message)};
  }
  
  public ProtocolStatus(Throwable t) {
    this(EXCEPTION, t);
  }

  public static ProtocolStatus read(DataInput in) throws IOException {
    ProtocolStatus res = new ProtocolStatus();
    res.readFields(in);
    return res;
  }
  
  public void readFields(DataInput in) throws IOException {
    code = in.readByte();
    args = WritableUtils.readCompressedStringArray(in);
  }
  
  public void write(DataOutput out) throws IOException {
    out.writeByte((byte)code);
    WritableUtils.writeCompressedStringArray(out, args);
  }

  public String[] getArgs() {
    return args;
  }

  public int getCode() {
    return code;
  }
  
  public boolean isSuccess() {
    return code == SUCCESS; 
  }
  
  public String getMessage() {
    if (args != null && args.length > 0) return args[0];
    return null;
  }
  
  public boolean equals(Object o) {
    if (o == null) return false;
    if (!(o instanceof ProtocolStatus)) return false;
    ProtocolStatus other = (ProtocolStatus)o;
    if (this.code != other.code) return false;
    if (this.args == null) {
      if (other.args == null) return true;
      else return false;
    } else {
      if (other.args == null) return false;
      if (other.args.length != this.args.length) return false;
      for (int i = 0; i < this.args.length; i++) {
        if (!this.args[i].equals(other.args[i])) return false;
      }
    }
    return true;
  }
  
  public String toString() {
    StringBuffer res = new StringBuffer();
    res.append("(" + code + ")");
    if (args != null) {
      if (args.length == 1) {
        res.append(": " + String.valueOf(args[0]));
      } else {
        for (int i = 0; i < args.length; i++) {
          if (args[i] != null)
            res.append(", args[" + i + "]=" + String.valueOf(args[i]));
        }
      }
    }
    return res.toString();
  }
}

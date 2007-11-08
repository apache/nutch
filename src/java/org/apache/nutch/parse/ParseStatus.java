/*
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
/*
 * Created on Apr 28, 2005
 * Author: Andrzej Bialecki &lt;ab@getopt.org&gt;
 *
 */
package org.apache.nutch.parse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.conf.Configuration;

import org.apache.nutch.metadata.Metadata;


/**
 * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
 */
public class ParseStatus implements Writable {
  
  private final static byte VERSION = 2;
  
  // Primary status codes:
  
  /** Parsing was not performed. */
  public static final byte NOTPARSED       = 0;
  /** Parsing succeeded. */
  public static final byte SUCCESS         = 1;
  /** General failure. There may be a more specific error message in arguments. */
  public static final byte FAILED          = 2;
  
  public static final String[] majorCodes = {
          "notparsed",
          "success",
          "failed"
  };
  
  // Secondary success codes go here:
  
  /** Parsed content contains a directive to redirect to another URL.
   * The target URL can be retrieved from the arguments.
   */
  public static final short SUCCESS_REDIRECT          = 100;
  
  // Secondary failure codes go here:
  
  /** Parsing failed. An Exception occured (which may be retrieved from the arguments). */
  public static final short FAILED_EXCEPTION          = 200;
  /** Parsing failed. Content was truncated, but the parser cannot handle incomplete content. */
  public static final short FAILED_TRUNCATED          = 202;
  /** Parsing failed. Invalid format - the content may be corrupted or of wrong type. */
  public static final short FAILED_INVALID_FORMAT     = 203;
  /** Parsing failed. Other related parts of the content are needed to complete
   * parsing. The list of URLs to missing parts may be provided in arguments.
   * The Fetcher may decide to fetch these parts at once, then put them into
   * Content.metadata, and supply them for re-parsing.
   */
  public static final short FAILED_MISSING_PARTS      = 204;
  /** Parsing failed. There was no content to be parsed - probably caused
   * by errors at protocol stage.
   */
  public static final short FAILED_MISSING_CONTENT    = 205;


  public static final ParseStatus STATUS_NOTPARSED = new ParseStatus(NOTPARSED);
  public static final ParseStatus STATUS_SUCCESS = new ParseStatus(SUCCESS);
  public static final ParseStatus STATUS_FAILURE = new ParseStatus(FAILED);
  
  private byte majorCode = 0;
  private short minorCode = 0;
  private String[] args = null;
  
  public byte getVersion() {
    return VERSION;
  }

  public ParseStatus() {
    
  }
  
  public ParseStatus(int majorCode, int minorCode, String[] args) {
    this.args = args;
    this.majorCode = (byte)majorCode;
    this.minorCode = (short)minorCode;
  }
  
  public ParseStatus(int majorCode) {
    this(majorCode, 0, (String[])null);
  }
  
  public ParseStatus(int majorCode, String[] args) {
    this(majorCode, 0, args);
  }
  
  public ParseStatus(int majorCode, int minorCode) {
    this(majorCode, minorCode, (String[])null);
  }
  
  /** Simplified constructor for passing just a text message. */
  public ParseStatus(int majorCode, int minorCode, String message) {
    this(majorCode, minorCode, new String[]{message});
  }
  
  /** Simplified constructor for passing just a text message. */
  public ParseStatus(int majorCode, String message) {
    this(majorCode, 0, new String[]{message});
  }
  
  public ParseStatus(Throwable t) {
    this(FAILED, FAILED_EXCEPTION, new String[]{t.toString()});
  }
  
  public static ParseStatus read(DataInput in) throws IOException {
    ParseStatus res = new ParseStatus();
    res.readFields(in);
    return res;
  }
  
  public void readFields(DataInput in) throws IOException {
    byte version = in.readByte();
    switch(version) {
    case 1:
      majorCode = in.readByte();
      minorCode = in.readShort();
      args = WritableUtils.readCompressedStringArray(in);
      break;
    case 2:
      majorCode = in.readByte();
      minorCode = in.readShort();
      args = WritableUtils.readStringArray(in);
      break;
    default:
      throw new VersionMismatchException(VERSION, version);
    }
 }
  
  public void write(DataOutput out) throws IOException {
    out.writeByte(VERSION);
    out.writeByte(majorCode);
    out.writeShort(minorCode);
    if (args == null) {
      out.writeInt(-1);
    } else {
      WritableUtils.writeStringArray(out, args);
    }
  }
  
  /** A convenience method. Returns true if majorCode is SUCCESS, false
   * otherwise.
   */
  
  public boolean isSuccess() {
    return majorCode == SUCCESS;
  }
  
  /** A convenience method. Return a String representation of the first
   * argument, or null.
   */
  public String getMessage() {
    if (args != null && args.length > 0 && args[0] != null)
      return args[0];
    return null;
  }
  
  public String[] getArgs() {
    return args;
  }
  
  public int getMajorCode() {
    return majorCode;
  }
  
  public int getMinorCode() {
    return minorCode;
  }
  
  /** A convenience method. Creates an empty Parse instance,
   * which returns this status.
   */
  public Parse getEmptyParse(Configuration conf) {
    return new EmptyParseImpl(this, conf);
  }
  
  /** A convenience method. Creates an empty ParseResult,
   * which contains this status.
   */
  public ParseResult getEmptyParseResult(String url, Configuration conf) {
    return ParseResult.createParseResult(url, getEmptyParse(conf));
  }
  
  public String toString() {
    StringBuffer res = new StringBuffer();
    String name = null;
    if (majorCode >= 0 && majorCode < majorCodes.length) name = majorCodes[majorCode];
    else name = "UNKNOWN!";
    res.append(name + "(" + majorCode + "," + minorCode + ")");
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
  
  public void setArgs(String[] args) {
    this.args = args;
  }
  
  public void setMessage(String msg) {
    if (args == null || args.length == 0) {
      args = new String[1];
    }
    args[0] = msg;
  }
  
  public void setMajorCode(byte majorCode) {
    this.majorCode = majorCode;
  }

  public void setMinorCode(short minorCode) {
    this.minorCode = minorCode;
  }
  
  public boolean equals(Object o) {
    if (o == null) return false;
    if (!(o instanceof ParseStatus)) return false;
    boolean res = true;
    ParseStatus other = (ParseStatus)o;
    res = res && (this.majorCode == other.majorCode) &&
      (this.minorCode == other.minorCode);
    if (!res) return res;
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
  
  private static class EmptyParseImpl implements Parse {
    
    private ParseData data = null;
    
    public EmptyParseImpl(ParseStatus status, Configuration conf) {
      data = new ParseData(status, "", new Outlink[0],
                           new Metadata(), new Metadata());
    }
    
    public ParseData getData() {
      return data;
    }

    public String getText() {
      return "";
    }
    
    public boolean isCanonical() {
      return true;
    }
  }
}


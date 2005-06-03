/* Copyright (c) 2003 The Nutch Organization.  All rights reserved.   */
/* Use subject to the conditions in http://www.nutch.org/LICENSE.txt. */

package org.apache.nutch.protocol.httpclient;

/** Thrown for HTTP error codes.
 */
public class HttpError extends HttpException {

  private int code;
  
  public int getCode(int code) { return code; }

  public HttpError(int code) {
    super("HTTP Error: " + code);
    this.code = code;
  }

}

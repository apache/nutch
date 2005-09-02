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
package org.apache.nutch.parse.mspowerpoint;

import java.io.IOException;

/**
 * Exception class used for catching the runtime exceptions for the Powerpoint
 * slides.
 * 
 * @author Stephan Strittmatter - http://www.sybit.de
 * 
 * @version 1.0
 */

public class PowerPointDocumentException extends Exception {

  /** Comment for <code>serialVersionUID</code> */
  private static final long serialVersionUID = 3256438093031487028L;

  /**
   * A constructor that builds the Exception object
   * 
   * @param message
   */
  public PowerPointDocumentException(String message) {
    super(message);
  }

  /**
   * A constructor that builds the Exception object
   * 
   * @param message
   * @param cause
   */
  public PowerPointDocumentException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * @param e
   */
  public PowerPointDocumentException(Exception e) {
    super(e);
  }
}
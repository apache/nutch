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

package org.apache.nutch.mapred;

import java.util.HashMap;

/** Repository of named {@link InputFormat}s. */
public class InputFormats {
  private static final HashMap NAME_TO_FORMAT = new HashMap();

  static {                                        // register with repository
    InputFormats.add(new TextInputFormat());
    InputFormats.add(new SequenceFileInputFormat());
  }

  private InputFormats() {}                       // no public ctor
  
  /** Return the named {@link InputFormat}. */
  public static synchronized InputFormat get(String name) {
    return (InputFormat)NAME_TO_FORMAT.get(name);
  }

  /** Define a named {@link InputFormat}.*/
  public static synchronized void add(InputFormat format) {
    NAME_TO_FORMAT.put(format.getName(), format);
  }
}

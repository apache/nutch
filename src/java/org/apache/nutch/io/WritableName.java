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

package org.apache.nutch.io;

import java.util.HashMap;
import java.io.IOException;

/** Utility to permit renaming of Writable implementation classes without
 * invalidiating files that contain their class name.
 * @author Doug Cutting
 */
public class WritableName {
  private static HashMap NAME_TO_CLASS = new HashMap();
  private static HashMap CLASS_TO_NAME = new HashMap();

  static {                                        // define important types
    WritableName.setName(NullWritable.class, "null");
    WritableName.setName(LongWritable.class, "long");
    WritableName.setName(UTF8.class, "UTF8");
    WritableName.setName(MD5Hash.class, "MD5Hash");
    WritableName.setName(org.apache.nutch.db.Page.class, "Page");
    WritableName.setName(org.apache.nutch.db.Link.class, "Link");
    WritableName.setName
      (org.apache.nutch.pagedb.FetchListEntry.class, "FetchListEntry");
    WritableName.setName
      (org.apache.nutch.fetcher.FetcherOutput.class, "FetcherOutput");
    WritableName.setName(org.apache.nutch.protocol.Content.class, "Content");
    WritableName.setName(org.apache.nutch.parse.ParseText.class, "ParseText");
    WritableName.setName(org.apache.nutch.parse.ParseData.class, "ParseData");
  }

  private WritableName() {}                      // no public ctor

  /** Set the name that a class should be known as to something other than the
   * class name. */
  public static synchronized void setName(Class writableClass, String name) {
    CLASS_TO_NAME.put(writableClass, name);
    NAME_TO_CLASS.put(name, writableClass);
  }

  /** Add an alternate name for a class. */
  public static synchronized void addName(Class writableClass, String name) {
    NAME_TO_CLASS.put(name, writableClass);
  }

  /** Return the name for a class.  Default is {@link Class#getName()}. */
  public static synchronized String getName(Class writableClass) {
    String name = (String)CLASS_TO_NAME.get(writableClass);
    if (name != null)
      return name;
    return writableClass.getName();
  }

  /** Return the class for a name.  Default is {@link Class#forName(String)}.*/
  public static synchronized Class getClass(String name) throws IOException {
    Class writableClass = (Class)NAME_TO_CLASS.get(name);
    if (writableClass != null)
      return writableClass;
    try {
      return Class.forName(name);
    } catch (ClassNotFoundException e) {
      throw new IOException(e.toString());
    }
  }

}

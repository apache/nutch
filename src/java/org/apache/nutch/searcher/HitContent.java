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

package org.apache.nutch.searcher;

import java.io.IOException;

import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;

/** Service that returns the content of a hit. */
public interface HitContent {
  /** Returns the content of a hit document. */
  byte[] getContent(HitDetails details) throws IOException;

  /** Returns the ParseData of a hit document. */
  ParseData getParseData(HitDetails details) throws IOException;

  /** Returns the ParseText of a hit document. */
  ParseText getParseText(HitDetails details) throws IOException;

  /** Returns the fetch date of a hit document. */
  long getFetchDate(HitDetails details) throws IOException;

}

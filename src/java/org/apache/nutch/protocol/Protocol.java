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

import java.io.IOException;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.io.UTF8;
import org.apache.nutch.util.NutchConfigurable;

/** A retriever of url content.  Implemented by protocol extensions. */
public interface Protocol extends NutchConfigurable {
  /** The name of the extension point. */
  public final static String X_POINT_ID = Protocol.class.getName();

  /** Returns the {@link Content} for a fetchlist entry.
   */
  ProtocolOutput getProtocolOutput(UTF8 url, CrawlDatum datum);
}

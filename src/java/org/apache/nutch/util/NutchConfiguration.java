/**
 * Copyright 2006 The Apache Software Foundation
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

package org.apache.nutch.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableName;

/** Utility to create Hadoop {@link Configuration}s that include Nutch-specific
 * resources.  */
public class NutchConfiguration {

  // for back-compatibility, add old aliases for these Writable classes
  // this may be removed after the 0.8 release
  static {
    WritableName.addName(org.apache.nutch.fetcher.FetcherOutput.class,
                         "FetcherOutput"); 
    WritableName.addName(org.apache.nutch.parse.ParseData.class, "ParseData"); 
    WritableName.addName(org.apache.nutch.parse.ParseText.class, "ParseText"); 
    WritableName.addName(org.apache.nutch.protocol.Content.class, "Content");
  }

  /** Create a {@link Configuration} for Nutch. */
  public static Configuration create() {
    Configuration conf = new Configuration();
    addNutchResources(conf);
    return conf;
  }

  /** Add the standard Nutch resources to {@link Configuration}. */
  public static Configuration addNutchResources(Configuration conf) {
    conf.addDefaultResource("nutch-default.xml");
    conf.addFinalResource("nutch-site.xml");
    return conf;
  }
}


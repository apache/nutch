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

package org.apache.nutch.crawl;

import java.net.URL;
import java.util.Iterator;
import java.io.IOException;

import org.apache.nutch.io.*;
import org.apache.nutch.mapred.*;

/** Merge new page entries with existing entries. */
public class CrawlDBReducer implements Reducer {

  public void configure(JobConf job) {}

  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output) throws IOException {
    // collect datum with the highest status
    CrawlDatum result = null;
    int linkCount = 0;
    while (values.hasNext()) {
      CrawlDatum datum = (CrawlDatum)values.next();
      linkCount += datum.getLinkCount();          // sum link counts
                                                  // keep w/ max status
      if (result == null || datum.getStatus() > result.getStatus())
        result = datum;
    }
    result.setLinkCount(linkCount);
    output.collect(key, result);
  }
}

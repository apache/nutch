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

package org.apache.nutch.db;

import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;
import java.util.logging.*;

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;
import org.apache.nutch.db.*;

/** Utility that extracts the set of anchor texts for a URL from the database.
 */
public class WebDBAnchors {
  private IWebDBReader db;
  private TreeMap anchorTable = new TreeMap();
  
  private static final Comparator DOMAIN_COMPARATOR = new Comparator() {
      public int compare(Object o1, Object o2) {
        Link l1 = (Link) o1;
        Link l2 = (Link) o2;
        if (l1.getDomainID() < l2.getDomainID()) {
          return -1;
        } else if (l1.getDomainID() == l2.getDomainID()) {
          return 0;
        } else {
          return 1;
        }
      }
    };

  /** Construct for the named db. */
  public WebDBAnchors(IWebDBReader db) {
    this.db = db;
  }
  
  /** Return the anchor texts of links in the db that point to this URL.
   * Performs duplicate elimination by source site, so that a single site
   * cannot generate multiple, identical anchors for a URL.  */
  public String[] getAnchors(UTF8 url) throws IOException {

    // Uniquify identical anchor text strings by source domain.  If the anchor
    // text is identical, and the domains are identical, then the anchor should
    // only be included once.
    Link links[] = db.getLinks(url);
    int uniqueAnchors = 0;
    for (int i = 0; i < links.length; i++) {
      Link link = links[i];
      if (link.getAnchorText().getLength() == 0)
        continue;
      String anchor = link.getAnchorText().toString();
      Set domainUniqueLinks = (Set)anchorTable.get(anchor);
      if (domainUniqueLinks == null) {
        domainUniqueLinks = new TreeSet(DOMAIN_COMPARATOR);
        anchorTable.put(anchor, domainUniqueLinks);
      }
      if (domainUniqueLinks.add(link)) {
        uniqueAnchors++;
      }
    }

    // Finally, collect the incoming anchor text for the current URL.
    int i = 0;
    String results[] = new String[uniqueAnchors];
    for (Iterator it = anchorTable.keySet().iterator(); it.hasNext(); ) {
      String key = (String)it.next();
      Set domainUniqueLinks = (Set)anchorTable.get(key);
      
      for (int j = 0; j < domainUniqueLinks.size(); j++) {
        results[i++] = key;
      }
    }
    anchorTable.clear();

    return results;
  }
}

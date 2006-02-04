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

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.io.*;

/** A list of {@link Inlink}s. */
public class Inlinks implements Writable {
  private ArrayList inlinks = new ArrayList(1);

  public void add(Inlink inlink) { inlinks.add(inlink); }

  public void add(Inlinks inlinks) { this.inlinks.addAll(inlinks.inlinks); }

  public int size() { return inlinks.size(); }

  public Inlink get(int i) { return (Inlink)inlinks.get(i); }

  public void clear() { inlinks.clear(); }

  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    inlinks.clear();
    inlinks.ensureCapacity(length);
    for (int i = 0; i < length; i++) {
      add(Inlink.read(in));
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(inlinks.size());
    for (int i = 0; i < inlinks.size(); i++) {
      ((Writable)inlinks.get(i)).write(out);
    }
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Inlinks:\n");
    for (int i = 0; i < inlinks.size(); i++) {
      buffer.append(" ");
      buffer.append(inlinks.get(i));
      buffer.append("\n");
    }
    return buffer.toString();
  }

  /** Return the set of anchor texts.  Only a single anchor with a given text
   * is permitted from a given domain. */
  public String[] getAnchors() throws IOException {
    HashMap domainToAnchors = new HashMap();
    ArrayList results = new ArrayList();
    for (int i = 0; i < inlinks.size(); i++) {
      Inlink inlink = (Inlink)inlinks.get(i);
      String anchor = inlink.getAnchor();

      if (anchor.length() == 0)                   // skip empty anchors
        continue;
      String domain = null;                       // extract domain name
      try {
        domain = new URL(inlink.getFromUrl()).getHost();
      } catch (MalformedURLException e) {}
      Set domainAnchors = (Set)domainToAnchors.get(domain);
      if (domainAnchors == null) {
        domainAnchors = new HashSet();
        domainToAnchors.put(domain, domainAnchors);
      }
      if (domainAnchors.add(anchor)) {            // new anchor from domain
        results.add(anchor);                      // collect it
      }
    }

    return (String[])results.toArray(new String[results.size()]);
  }


}

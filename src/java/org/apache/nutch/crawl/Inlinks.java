/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
  private HashSet<Inlink> inlinks = new HashSet<Inlink>(1);

  public void add(Inlink inlink) { inlinks.add(inlink); }

  public void add(Inlinks inlinks) { this.inlinks.addAll(inlinks.inlinks); }

  public Iterator<Inlink> iterator() {
    return this.inlinks.iterator();
  }
  
  public int size() { return inlinks.size(); }

  public void clear() { inlinks.clear(); }

  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    inlinks.clear();
    for (int i = 0; i < length; i++) {
      add(Inlink.read(in));
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(inlinks.size());
    Iterator<Inlink> it = inlinks.iterator();
    while (it.hasNext()) {
      it.next().write(out);
    }
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Inlinks:\n");
    Iterator<Inlink> it = inlinks.iterator();
    while (it.hasNext()) {
      buffer.append(" ");
      buffer.append(it.next());
      buffer.append("\n");
    }
    return buffer.toString();
  }

  /** Return the set of anchor texts.  Only a single anchor with a given text
   * is permitted from a given domain. */
  public String[] getAnchors() throws IOException {
    HashMap<String, Set<String>> domainToAnchors =
      new HashMap<String, Set<String>>();
    ArrayList<String> results = new ArrayList<String>();
    Iterator<Inlink> it = inlinks.iterator();
    while (it.hasNext()) {
      Inlink inlink = it.next();
      String anchor = inlink.getAnchor();

      if (anchor.length() == 0)                   // skip empty anchors
        continue;
      String domain = null;                       // extract domain name
      try {
        domain = new URL(inlink.getFromUrl()).getHost();
      } catch (MalformedURLException e) {}
      Set<String> domainAnchors = domainToAnchors.get(domain);
      if (domainAnchors == null) {
        domainAnchors = new HashSet<String>();
        domainToAnchors.put(domain, domainAnchors);
      }
      if (domainAnchors.add(anchor)) {            // new anchor from domain
        results.add(anchor);                      // collect it
      }
    }

    return results.toArray(new String[results.size()]);
  }


}

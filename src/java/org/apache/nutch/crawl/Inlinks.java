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
import java.util.*;

import org.apache.nutch.io.*;

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

}

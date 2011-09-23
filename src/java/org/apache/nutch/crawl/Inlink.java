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
import org.apache.hadoop.io.*;

/* An incoming link to a page. */
public class Inlink implements Writable {

  private String fromUrl;
  private String anchor;

  public Inlink() {}

  public Inlink(String fromUrl, String anchor) {
    this.fromUrl = fromUrl;
    this.anchor = anchor;
  }

  public void readFields(DataInput in) throws IOException {
    fromUrl = Text.readString(in);
    anchor = Text.readString(in);
  }

  /** Skips over one Inlink in the input. */
  public static void skip(DataInput in) throws IOException {
    Text.skip(in);                                // skip fromUrl
    Text.skip(in);                                // skip anchor
  }

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, fromUrl);
    Text.writeString(out, anchor);
  }

  public static Inlink read(DataInput in) throws IOException {
    Inlink inlink = new Inlink();
    inlink.readFields(in);
    return inlink;
  }

  public String getFromUrl() { return fromUrl; }
  public String getAnchor() { return anchor; }

  public boolean equals(Object o) {
    if (!(o instanceof Inlink))
      return false;
    Inlink other = (Inlink)o;
    return
      this.fromUrl.equals(other.fromUrl) &&
      this.anchor.equals(other.anchor);
  }

  public int hashCode() {
    return fromUrl.hashCode() ^ anchor.hashCode();
  }

  public String toString() {
    return "fromUrl: " + fromUrl + " anchor: " + anchor;
  }

}

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

package org.apache.nutch.parse;

import java.io.*;
import java.net.MalformedURLException;

import org.apache.hadoop.io.*;
import org.apache.nutch.net.URLNormalizers;
import org.apache.hadoop.conf.Configuration;

/* An outgoing link from a page. */
public class Outlink implements Writable {

  private String toUrl;
  private String anchor;

  public Outlink() {}

  public Outlink(String toUrl, String anchor) throws MalformedURLException {
    this.toUrl = toUrl;
    if (anchor == null) anchor = "";
    this.anchor = anchor;
  }

  public void readFields(DataInput in) throws IOException {
    toUrl = Text.readString(in);
    anchor = Text.readString(in);
  }

  /** Skips over one Outlink in the input. */
  public static void skip(DataInput in) throws IOException {
    Text.skip(in);                                // skip toUrl
    Text.skip(in);                                // skip anchor
  }

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, toUrl);
    Text.writeString(out, anchor);
  }

  public static Outlink read(DataInput in) throws IOException {
    Outlink outlink = new Outlink();
    outlink.readFields(in);
    return outlink;
  }

  public String getToUrl() { return toUrl; }
  public String getAnchor() { return anchor; }


  public boolean equals(Object o) {
    if (!(o instanceof Outlink))
      return false;
    Outlink other = (Outlink)o;
    return
      this.toUrl.equals(other.toUrl) &&
      this.anchor.equals(other.anchor);
  }

  public String toString() {
    return "toUrl: " + toUrl + " anchor: " + anchor;  // removed "\n". toString, not printLine... WD.
  }

}

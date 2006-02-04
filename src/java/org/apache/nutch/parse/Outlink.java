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

package org.apache.nutch.parse;

import java.io.*;
import java.net.MalformedURLException;

import org.apache.hadoop.io.*;
import org.apache.nutch.net.UrlNormalizerFactory;
import org.apache.hadoop.conf.Configuration;

/* An outgoing link from a page. */
public class Outlink implements Writable {

  private String toUrl;
  private String anchor;

  public Outlink() {}

  public Outlink(String toUrl, String anchor, Configuration conf) throws MalformedURLException {
    this.toUrl = new UrlNormalizerFactory(conf).getNormalizer().normalize(toUrl);
    this.anchor = anchor;
  }

  public void readFields(DataInput in) throws IOException {
    toUrl = UTF8.readString(in);
    anchor = UTF8.readString(in);
  }

  /** Skips over one Outlink in the input. */
  public static void skip(DataInput in) throws IOException {
    UTF8.skip(in);                                // skip toUrl
    UTF8.skip(in);                                // skip anchor
  }

  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, toUrl);
    UTF8.writeString(out, anchor);
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

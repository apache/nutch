/*
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
package org.commoncrawl.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.Content;

/**
 * Container to hold all Nutch objects related to a single page capture and
 * necessary to write the WARC request and response records.
 */
public class WarcCapture implements Writable {
  public Text url;
  public CrawlDatum datum;
  public Content content;

  public WarcCapture() {
    url = new Text();
    datum = new CrawlDatum();
    content = new Content();
  }

  public WarcCapture(Text url, CrawlDatum datum, Content content) {
    this.url = url;
    this.datum = datum;
    this.content = content;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    url.readFields(in);
    if (in.readBoolean()) {
      datum.readFields(in);
    } else {
      datum = null;
    }
    if (in.readBoolean()) {
      content.readFields(in);
    } else {
      content = null;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    url.write(out);
    if (datum != null) {
      out.writeBoolean(true);
      datum.write(out);
    } else {
      out.writeBoolean(false);
    }
    if (content != null) {
      out.writeBoolean(true);
      content.write(out);
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public String toString() {
    return "url=" + url.toString() + ", datum=" + datum.toString();
  }
}
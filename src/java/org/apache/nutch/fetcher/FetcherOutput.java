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

package org.apache.nutch.fetcher;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.parse.*;

/* An entry in the fetcher's output. */
public final class FetcherOutput implements Writable {
  private CrawlDatum crawlDatum;
  private Content content;
  private ParseImpl parse;

  public FetcherOutput() {}

  public FetcherOutput(CrawlDatum crawlDatum, Content content,
                       ParseImpl parse) {
    this.crawlDatum = crawlDatum;
    this.content = content;
    this.parse = parse;
  }

  public final void readFields(DataInput in) throws IOException {
    this.crawlDatum = CrawlDatum.read(in);
    this.content = in.readBoolean() ? Content.read(in) : null;
    this.parse = in.readBoolean() ? ParseImpl.read(in) : null;
  }

  public final void write(DataOutput out) throws IOException {
    crawlDatum.write(out);

    out.writeBoolean(content != null);
    if (content != null) {
      content.write(out);
    }

    out.writeBoolean(parse != null);
    if (parse != null) {
      parse.write(out);
    }
  }

  public CrawlDatum getCrawlDatum() { return crawlDatum; }
  public Content getContent() { return content; }
  public ParseImpl getParse() { return parse; }

  public boolean equals(Object o) {
    if (!(o instanceof FetcherOutput))
      return false;
    FetcherOutput other = (FetcherOutput)o;
    return
      this.crawlDatum.equals(other.crawlDatum) &&
      this.content.equals(other.content);
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("CrawlDatum: " + crawlDatum+"\n" );
    return buffer.toString();
  }

}

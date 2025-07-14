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
package org.apache.nutch.indexer.arbitrary;

import org.apache.nutch.parse.Parse;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.hadoop.io.Text;

import java.io.PrintStream;

public class UpdatedEcho {

  private NutchDocument doc;
  private Parse parse;
  private Text url;
  private CrawlDatum datum;
  private Inlinks inlinks;
  private static PrintStream out = System.out;
  private String words;

  public UpdatedEcho(String args[],
                     NutchDocument docIn,
                     Parse parseIn,
                     Text urlIn,
                     CrawlDatum datumIn,
                     Inlinks inlinksIn) {
    super();
    doc = docIn;
    parse = parseIn;
    url = urlIn;
    inlinks = inlinksIn;
    datum = datumIn;
  }

  public UpdatedEcho(String args[]) {
    super();
    words = String.valueOf(args[1]);
  }

  public String getText() {
    return words;
  }
}

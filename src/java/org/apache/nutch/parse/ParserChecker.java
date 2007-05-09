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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.nutch.util.NutchConfiguration;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.hadoop.io.Text;
import org.apache.nutch.parse.ParseUtil;

import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.Content;

/**
 * Parser checker, useful for testing parser.
 * 
 * @author John Xing
 */

public class ParserChecker {

  public static final Log LOG = LogFactory.getLog(ParserChecker.class);

  public ParserChecker() {}

  public static void main(String[] args) throws Exception {
    boolean dumpText = false;
    boolean force = false;
    String contentType = null;
    String url = null;

    String usage = "Usage: ParserChecker [-dumpText] [-forceAs mimeType] url";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-forceAs")) {
        force = true;
        contentType = args[++i];
      } else if (args[i].equals("-dumpText")) {
        dumpText = true;
      } else if (i != args.length-1) {
        System.err.println(usage);
        System.exit(-1);
      } else {
        url = args[i];
      }
    }

    if (LOG.isInfoEnabled()) { LOG.info("fetching: "+url); }

    Configuration conf = NutchConfiguration.create();
    ProtocolFactory factory = new ProtocolFactory(conf);
    Protocol protocol = factory.getProtocol(url);
    Content content = protocol.getProtocolOutput(new Text(url), new CrawlDatum()).getContent();

    if (force) {
      content.setContentType(contentType);
    } else {
      contentType = content.getContentType();
    }

    if (contentType == null) {
      System.err.println("");
      System.exit(-1);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("parsing: "+url);
      LOG.info("contentType: "+contentType);
    }

    ParseResult parseResult = new ParseUtil(conf).parse(content);

    for (java.util.Map.Entry<Text, Parse> entry : parseResult) {
      Parse parse = entry.getValue();
      System.out.print("---------\nUrl\n---------------\n");
      System.out.print(entry.getKey());
      System.out.print("---------\nParseData\n---------\n");
      System.out.print(parse.getData().toString());
      if (dumpText) {
        System.out.print("---------\nParseText\n---------\n");
        System.out.print(parse.getText());
      }
    }

    System.exit(0);
  }
}

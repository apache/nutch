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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.util.NutchConfiguration;

/**
 * Parser checker, useful for testing parser.
 * 
 * @author John Xing
 */

public class ParserChecker implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(ParserChecker.class);

  public ParserChecker() {
  }

  Configuration conf = null;

  public int run(String[] args) throws Exception {
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
      } else if (i != args.length - 1) {
        System.err.println(usage);
        System.exit(-1);
      } else {
        url = args[i];
      }
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("fetching: " + url);
    }

    ProtocolFactory factory = new ProtocolFactory(conf);
    Protocol protocol = factory.getProtocol(url);
    Content content = protocol.getProtocolOutput(new Text(url),
        new CrawlDatum()).getContent();

    if (content == null) {
      System.err.println("Can't fetch URL successfully");
      return (-1);
    }

    if (force) {
      content.setContentType(contentType);
    } else {
      contentType = content.getContentType();
    }

    if (contentType == null) {
      System.err.println("");
      return (-1);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("parsing: " + url);
      LOG.info("contentType: " + contentType);
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

    return 0;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration c) {
    conf = c;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new ParserChecker(),
        args);
    System.exit(res);
  }

}

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

import org.apache.nutch.util.LogFormatter;

import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.Content;

import java.util.logging.Logger;

/**
 * Parser checker, useful for testing parser.
 * 
 * @author John Xing
 */

public class ParserChecker {

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.parse.ParserChecker");

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

    LOG.info("fetching: "+url);

    Protocol protocol = ProtocolFactory.getProtocol(url);
    Content content = protocol.getContent(url);

    if (force) {
      content.setContentType(contentType);
    } else {
      contentType = content.getContentType();
    }

    if (contentType == null) {
      System.err.println("");
      System.exit(-1);
    }

    LOG.info("parsing: "+url);
    LOG.info("contentType: "+contentType);

    Parser parser = ParserFactory.getParser(contentType, url);
    Parse parse = parser.getParse(content);

    System.out.print("---------\nParseData\n---------\n");
    System.out.print(parse.getData().toString());
    if (dumpText) {
      System.out.print("---------\nParseText\n---------\n");
      System.out.print(parse.getText());
    }

    System.exit(0);
  }
}

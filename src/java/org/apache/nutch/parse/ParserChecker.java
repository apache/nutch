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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;

/**
 * Parser checker, useful for testing parser.
 * 
 * @author John Xing
 */

public class ParserChecker {

  public static final Log LOG = LogFactory.getLog(ParserChecker.class);

  public ParserChecker() {
  }

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

    Configuration conf = NutchConfiguration.create();
    ProtocolFactory factory = new ProtocolFactory(conf);
    Protocol protocol = factory.getProtocol(url);
    WebPage page = new WebPage();
    Content content = protocol.getProtocolOutput(url, page).getContent();
    page.setBaseUrl(new org.apache.avro.util.Utf8(url));
    page.setContent(ByteBuffer.wrap(content.getContent()));
    
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
      LOG.info("parsing: " + url);
      LOG.info("contentType: " + contentType);
    }
    
    page.setContentType(new Utf8(contentType));

    Parse parse = new ParseUtil(conf).parse(url, page);

    if (parse==null){
      System.err.println("Problem with parse - check log");
      System.exit(-1);
    }
    
    System.out.print("---------\nUrl\n---------------\n");
    System.out.print(url+"\n");
    System.out.print("---------\nMetadata\n---------\n");
    Map<Utf8, ByteBuffer> metadata = page.getMetadata();
    StringBuffer sb = new StringBuffer();
    if (metadata != null) {
      Iterator<Entry<Utf8, ByteBuffer>> iterator = metadata.entrySet()
          .iterator();
      while (iterator.hasNext()) {
        Entry<Utf8, ByteBuffer> entry = iterator.next();
        sb.append(entry.getKey().toString()).append(" : \t")
            .append(Bytes.toString(entry.getValue().array())).append("\n");
      }
      System.out.print(sb.toString());
    }
    if (dumpText) {
      System.out.print("---------\nParseText\n---------\n");
      System.out.print(parse.getText());
    }

    System.exit(0);
  }
}

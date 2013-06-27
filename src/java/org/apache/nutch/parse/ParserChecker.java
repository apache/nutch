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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatusUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;

/**
 * Parser checker, useful for testing parser.
 * It also accurately reports possible fetching and 
 * parsing failures and presents protocol status signals to aid 
 * debugging. The tool enables us to retrieve the following data from 
 * any url:
 * <ol>
 * <li><tt>contentType</tt>: The URL {@link org.apache.nutch.protocol.Content} type.</li>
 * <li><tt>signature</tt>: Digest is used to identify pages (like unique ID) and is used to remove
 * duplicates during the dedup procedure. 
 * It is calculated using {@link org.apache.nutch.crawl.MD5Signature} or
 * {@link org.apache.nutch.crawl.TextProfileSignature}.</li>
 * <li><tt>Version</tt>: From {@link org.apache.nutch.parse.ParseData}.</li>
 * <li><tt>Status</tt>: From {@link org.apache.nutch.parse.ParseData}.</li>
 * <li><tt>Title</tt>: of the URL</li>
 * <li><tt>Outlinks</tt>: associated with the URL</li>
 * <li><tt>Content Metadata</tt>: such as <i>X-AspNet-Version</i>, <i>Date</i>,
 * <i>Content-length</i>, <i>servedBy</i>, <i>Content-Type</i>, <i>Cache-Control</>, etc.</li>
 * <li><tt>Parse Metadata</tt>: such as <i>CharEncodingForConversion</i>,
 * <i>OriginalCharEncoding</i>, <i>language</i>, etc.</li>
 * <li><tt>ParseText</tt>: The page parse text which varies in length depdnecing on 
 * <code>content.length</code> configuration.</li>
 * </ol>
 * @author John Xing
 */

public class ParserChecker implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(ParserChecker.class);
  private Configuration conf;

  public ParserChecker() {
  }

  public int run(String[] args) throws Exception {
    boolean dumpText = false;
    boolean force = false;
    String contentType = null;
    String url = null;

    String usage = "Usage: ParserChecker [-dumpText] [-forceAs mimeType] url";

    if (args.length == 0) {
      LOG.error(usage);
      return (-1);
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-forceAs")) {
        force = true;
        contentType = args[++i];
      } else if (args[i].equals("-dumpText")) {
        dumpText = true;
      } else if (i != args.length - 1) {
        LOG.error(usage);
        System.exit(-1);
      } else {
        url = URLUtil.toASCII(args[i]);
      }
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("fetching: " + url);
    }

    ProtocolFactory factory = new ProtocolFactory(conf);
    Protocol protocol = factory.getProtocol(url);
    WebPage page = new WebPage();
    
    ProtocolOutput protocolOutput = protocol.getProtocolOutput(url, page);
    
    if(!protocolOutput.getStatus().isSuccess()) {
      LOG.error("Fetch failed with protocol status: "
          + ProtocolStatusUtils.getName(protocolOutput.getStatus().getCode())
          + ": " + ProtocolStatusUtils.getMessage(protocolOutput.getStatus()));
      return (-1);
    }
    Content content = protocolOutput.getContent();
    
    if (content == null) {
      LOG.error("No content for " + url);
      return (-1);
    }
    page.setBaseUrl(new org.apache.avro.util.Utf8(url));
    page.setContent(ByteBuffer.wrap(content.getContent()));

    if (force) {
      content.setContentType(contentType);
    } else {
      contentType = content.getContentType();
    }

    if (contentType == null) {
      LOG.error("Failed to determine content type!");
      return (-1);
    }

    page.setContentType(new Utf8(contentType));

    if (ParserJob.isTruncated(url, page)) {
      LOG.warn("Content is truncated, parse may fail!");
    }

    Parse parse = new ParseUtil(conf).parse(url, page);

    if (parse == null) {
      LOG.error("Problem with parse - check log");
      return (-1);
    }
    
    // Calculate the signature
    byte[] signature = SignatureFactory.getSignature(getConf()).calculate(page);
    
    if (LOG.isInfoEnabled()) {
      LOG.info("parsing: " + url);
      LOG.info("contentType: " + contentType);
      LOG.info("signature: " + StringUtil.toHexString(signature));
    }


    LOG.info("---------\nUrl\n---------------\n");
    System.out.print(url + "\n");
    LOG.info("---------\nMetadata\n---------\n");
    Map<Utf8, ByteBuffer> metadata = page.getMetadata();
    StringBuffer sb = new StringBuffer();
    if (metadata != null) {
      Iterator<Entry<Utf8, ByteBuffer>> iterator = metadata.entrySet()
          .iterator();
      while (iterator.hasNext()) {
        Entry<Utf8, ByteBuffer> entry = iterator.next();
        sb.append(entry.getKey().toString()).append(" : \t")
            .append(Bytes.toString(entry.getValue())).append("\n");
      }
      System.out.print(sb.toString());
    }
    if (dumpText) {
      LOG.info("---------\nParseText\n---------\n");
      System.out.print(parse.getText());
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

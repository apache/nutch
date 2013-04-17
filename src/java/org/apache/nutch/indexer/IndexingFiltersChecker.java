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
 
package org.apache.nutch.indexer;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatusCodes;
import org.apache.nutch.protocol.ProtocolStatusUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads and parses a URL and run the indexers on it. Displays the fields obtained and the first
 * 100 characters of their value
 *
 * Tested with e.g. ./nutch org.apache.nutch.indexer.IndexingFiltersChecker http://www.lemonde.fr
 * @author Julien Nioche
 **/

public class IndexingFiltersChecker extends Configured implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(IndexingFiltersChecker.class);

  public IndexingFiltersChecker() {

  }

  public int run(String[] args) throws Exception {
    String contentType = null;
    String url = null;

    String usage = "Usage: IndexingFiltersChecker <url>";

    if (args.length != 1) {
      System.err.println(usage);
      return -1;
    }

    url = URLUtil.toASCII(args[0]);

    if (LOG.isInfoEnabled()) {
      LOG.info("fetching: " + url);
    }

    IndexingFilters indexers = new IndexingFilters(conf);

    ProtocolFactory factory = new ProtocolFactory(conf);
    Protocol protocol = factory.getProtocol(url);

    WebPage page = new WebPage();
    page.setBaseUrl(new org.apache.avro.util.Utf8(url));
    ProtocolOutput protocolOutput = protocol.getProtocolOutput(url, page);
    page.setProtocolStatus(protocolOutput.getStatus());
    if (protocolOutput.getStatus().getCode() == ProtocolStatusCodes.SUCCESS) {
      page.setStatus(CrawlStatus.STATUS_FETCHED);
      page.setFetchTime(System.currentTimeMillis());
    } else {
      LOG.error("Fetch failed with protocol status: "
          + ProtocolStatusUtils.getName(protocolOutput.getStatus().getCode())
          + ": " + ProtocolStatusUtils.getMessage(protocolOutput.getStatus()));
      return -1;
    }
    
    Content content = protocolOutput.getContent();
    if (content == null) {
      LOG.warn("No content for " + url);
      return 0;
    }

    page.setContent(ByteBuffer.wrap(content.getContent()));
    contentType = content.getContentType();
    if (contentType == null) {
      return -1;
    }
    page.setContentType(new Utf8(contentType));
    
    if (LOG.isInfoEnabled()) {
      LOG.info("parsing: " + url);
      LOG.info("contentType: " + contentType);
    }

    if (ParserJob.isTruncated(url, page)) {
      LOG.warn("Content is truncated, parse may fail!");
    }

    (new ParseUtil(conf)).process(url, page);
    if (!ParseStatusUtils.isSuccess(page.getParseStatus())) {
      LOG.warn("Problem with parse - check log");
      return (-1);
    }

    NutchDocument doc = new NutchDocument();

    try {
      doc = indexers.filter(doc, url, page);
    } catch (IndexingException e) {
      e.printStackTrace();
    }

    if (doc == null) {
      LOG.info("Document discarded by indexing filter");
      return 0;
    }
    
    for (String fname : doc.getFieldNames()) {
      List<String> values = doc.getFieldValues(fname);
      if (values != null) {
        for (Object value : values) {
          String str = value.toString();
          int minText = Math.min(100, str.length());
          System.out.println(fname + " :\t" + str.substring(0, minText));
        }
      }
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new IndexingFiltersChecker(), args);
    System.exit(res);
  }

  Configuration conf;

  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration arg0) {
    conf = arg0;
  }
}

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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads and parses a URL and run the indexers on it. Displays the fields
 * obtained and the first 100 characters of their value
 * 
 * Tested with e.g. ./nutch org.apache.nutch.indexer.IndexingFiltersChecker
 * http://www.lemonde.fr
 * 
 * @author Julien Nioche
 **/

public class IndexingFiltersChecker extends Configured implements Tool {

  public static final Logger LOG = LoggerFactory
      .getLogger(IndexingFiltersChecker.class);

  public IndexingFiltersChecker() {

  }

  public int run(String[] args) throws Exception {
    String contentType = null;
    String url = null;
    boolean dumpText = false;

    String usage = "Usage: IndexingFiltersChecker [-dumpText] [-md key=value] <url>";

    if (args.length == 0) {
      System.err.println(usage);
      return -1;
    }

    // used to simulate the metadata propagated from injection
    HashMap<String, String> metadata = new HashMap<String, String>();

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-dumpText")) {
        dumpText = true;
      } else if (args[i].equals("-md")) {
        String k = null, v = null;
        String nextOne = args[++i];
        int firstEquals = nextOne.indexOf("=");
        if (firstEquals != -1) {
          k = nextOne.substring(0, firstEquals);
          v = nextOne.substring(firstEquals + 1);
        } else
          k = nextOne;
        metadata.put(k, v);
      } else if (i != args.length - 1) {
        System.err.println(usage);
        System.exit(-1);
      } else {
        url = URLUtil.toASCII(args[i]);
      }
    }

    LOG.info("fetching: " + url);

    CrawlDatum datum = new CrawlDatum();

    Iterator<String> iter = metadata.keySet().iterator();
    while (iter.hasNext()) {
      String key = iter.next();
      String value = metadata.get(key);
      if (value == null)
        value = "";
      datum.getMetaData().put(new Text(key), new Text(value));
    }

    IndexingFilters indexers = new IndexingFilters(getConf());

    ProtocolFactory factory = new ProtocolFactory(getConf());
    Protocol protocol = factory.getProtocol(url);
    Text turl = new Text(url);
    ProtocolOutput output = protocol.getProtocolOutput(turl, datum);

    if (!output.getStatus().isSuccess()) {
      System.out.println("Fetch failed with protocol status: "
          + output.getStatus());
      return 0;
    }

    Content content = output.getContent();

    if (content == null) {
      System.out.println("No content for " + url);
      return 0;
    }

    contentType = content.getContentType();

    if (contentType == null) {
      return -1;
    }

    // store the guessed content type in the crawldatum
    datum.getMetaData().put(new Text(Metadata.CONTENT_TYPE),
        new Text(contentType));

    if (ParseSegment.isTruncated(content)) {
      LOG.warn("Content is truncated, parse may fail!");
    }

    ScoringFilters scfilters = new ScoringFilters(getConf());
    // call the scoring filters
    try {
      scfilters.passScoreBeforeParsing(turl, datum, content);
    } catch (Exception e) {
      LOG.warn("Couldn't pass score, url {} ({})", url, e);
    }

    LOG.info("parsing: {}", url);
    LOG.info("contentType: {}", contentType);

    ParseResult parseResult = new ParseUtil(getConf()).parse(content);

    NutchDocument doc = new NutchDocument();
    doc.add("id", url);
    Text urlText = new Text(url);

    Inlinks inlinks = null;
    Parse parse = parseResult.get(urlText);
    if (parse == null) {
      LOG.error("Failed to get parse from parse result");
      LOG.error("Available parses in parse result (by URL key):");
      for (Map.Entry<Text, Parse> entry : parseResult) {
        LOG.error("  " + entry.getKey());
      }
      LOG.error("Parse result does not contain a parse for URL to be checked:");
      LOG.error("  " + urlText);
      return -1;
    }

    byte[] signature = SignatureFactory.getSignature(getConf()).calculate(content,
        parse);
    parse.getData().getContentMeta()
        .set(Nutch.SIGNATURE_KEY, StringUtil.toHexString(signature));
    String digest = parse.getData().getContentMeta().get(Nutch.SIGNATURE_KEY);
    doc.add("digest", digest);

    // call the scoring filters
    try {
      scfilters.passScoreAfterParsing(turl, content, parseResult.get(turl));
    } catch (Exception e) {
      LOG.warn("Couldn't pass score, url {} ({})", turl, e);
    }

    try {
      doc = indexers.filter(doc, parse, urlText, datum, inlinks);
    } catch (IndexingException e) {
      e.printStackTrace();
    }

    if (doc == null) {
      System.out.println("Document discarded by indexing filter");
      return 0;
    }

    for (String fname : doc.getFieldNames()) {
      List<Object> values = doc.getField(fname).getValues();
      if (values != null) {
        for (Object value : values) {
          String str = value.toString();
          int minText = dumpText ? str.length() : Math.min(100, str.length());
          System.out.println(fname + " :\t" + str.substring(0, minText));
        }
      }
    }

    if (getConf().getBoolean("doIndex", false) && doc != null) {
      IndexWriters writers = new IndexWriters(getConf());
      writers.open(new JobConf(getConf()), "IndexingFilterChecker");
      writers.write(doc);
      writers.close();
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new IndexingFiltersChecker(), args);
    System.exit(res);
  }
}

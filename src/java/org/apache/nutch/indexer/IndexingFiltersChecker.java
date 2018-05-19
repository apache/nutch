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

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.AbstractChecker;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads and parses a URL and run the indexers on it. Displays the fields
 * obtained and the first 100 characters of their value
 * 
 * Tested with e.g.
 * 
 * <pre>
    echo "http://www.lemonde.fr" | $NUTCH_HOME/bin/nutch indexchecker -stdin
 * </pre>
 **/
public class IndexingFiltersChecker extends AbstractChecker {

  protected URLNormalizers normalizers = null;
  protected boolean dumpText = false;
  protected boolean followRedirects = false;
  // used to simulate the metadata propagated from injection
  protected HashMap<String, String> metadata = new HashMap<>();

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public int run(String[] args) throws Exception {
    String url = null;

    usage = "Usage: IndexingFiltersChecker [-normalize] [-followRedirects] [-dumpText] [-md key=value] (-stdin | -listen <port> [-keepClientCnxOpen])";

    // Print help when no args given
    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }

    int numConsumed;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-normalize")) {
        normalizers = new URLNormalizers(getConf(), URLNormalizers.SCOPE_DEFAULT);
      } else if (args[i].equals("-followRedirects")) {
        followRedirects = true;
      } else if (args[i].equals("-dumpText")) {
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
      } else if ((numConsumed = super.parseArgs(args, i)) > 0) {
        i += numConsumed - 1;
      } else if (i != args.length - 1) {
        System.err.println("ERR: Not a recognized argument: " + args[i]);
        System.err.println(usage);
        System.exit(-1);
      } else {
        url = args[i];
      }
    }
    
    if (url != null) {
      return super.processSingle(url);
    } else {
      // Start listening
      return super.run();
    }
  }

  protected int process(String url, StringBuilder output) throws Exception {
    if (normalizers != null) {
      url = normalizers.normalize(url, URLNormalizers.SCOPE_DEFAULT);
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

    int maxRedirects = getConf().getInt("http.redirect.max", 3);
    if (followRedirects) {
      if (maxRedirects == 0) {
        LOG.info("Following max. 3 redirects (ignored http.redirect.max == 0)");
        maxRedirects = 3;
      } else {
        LOG.info("Following max. {} redirects", maxRedirects);
      }
    }

    ProtocolOutput protocolOutput = getProtocolOutput(url, datum);
    Text turl = new Text(url);
    
    // Following redirects and not reached maxRedirects?
    int numRedirects = 0;
    while (!protocolOutput.getStatus().isSuccess() && followRedirects
        && protocolOutput.getStatus().isRedirect() && maxRedirects >= numRedirects) {
      String[] stuff = protocolOutput.getStatus().getArgs();
      url = stuff[0];
      LOG.info("Follow redirect to {}", url);

      if (normalizers != null) {
        url = normalizers.normalize(url, URLNormalizers.SCOPE_DEFAULT);
      }

      turl.set(url);

      // try again
      protocolOutput = getProtocolOutput(url, datum);
      numRedirects++;
    }

    if (!protocolOutput.getStatus().isSuccess()) {
      System.err.println("Fetch failed with protocol status: "
          + protocolOutput.getStatus());

      if (protocolOutput.getStatus().isRedirect()) {
          System.err.println("Redirect(s) not handled due to configuration.");
          System.err.println("Max Redirects to handle per config: " + maxRedirects);
          System.err.println("Number of Redirects handled: " + numRedirects);
      }
      return -1;
    }

    Content content = protocolOutput.getContent();

    if (content == null) {
      output.append("No content for " + url + "\n");
      return 0;
    }

    String contentType = content.getContentType();

    if (contentType == null) {
      LOG.error("Failed to determine content type!");
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
    datum.setSignature(signature);

    // call the scoring filters
    try {
      scfilters.passScoreAfterParsing(turl, content, parseResult.get(turl));
    } catch (Exception e) {
      LOG.warn("Couldn't pass score, url {} ({})", turl, e);
    }

    IndexingFilters indexers = new IndexingFilters(getConf());

    try {
      doc = indexers.filter(doc, parse, urlText, datum, inlinks);
    } catch (IndexingException e) {
      e.printStackTrace();
    }

    if (doc == null) {
      output.append("Document discarded by indexing filter\n");
      return 0;
    }

    for (String fname : doc.getFieldNames()) {
      List<Object> values = doc.getField(fname).getValues();
      if (values != null) {
        for (Object value : values) {
          String str = value.toString();
          int minText = dumpText ? str.length() : Math.min(100, str.length());
          output.append(fname + " :\t" + str.substring(0, minText) + "\n");
        }
      }
    }
    
    output.append("\n"); // For readability if keepClientCnxOpen

    if (getConf().getBoolean("doIndex", false) && doc != null) {
      IndexWriters writers = new IndexWriters(getConf());
      writers.open(getConf(), "IndexingFilterChecker");
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

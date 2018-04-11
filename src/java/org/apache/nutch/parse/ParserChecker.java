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

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.AbstractChecker;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parser checker, useful for testing parser. It also accurately reports
 * possible fetching and parsing failures and presents protocol status signals
 * to aid debugging. The tool enables us to retrieve the following data from any
 * url:
 * <ol>
 * <li><tt>contentType</tt>: The URL {@link org.apache.nutch.protocol.Content}
 * type.</li>
 * <li><tt>signature</tt>: Digest is used to identify pages (like unique ID) and
 * is used to remove duplicates during the dedup procedure. It is calculated
 * using {@link org.apache.nutch.crawl.MD5Signature} or
 * {@link org.apache.nutch.crawl.TextProfileSignature}.</li>
 * <li><tt>Version</tt>: From {@link org.apache.nutch.parse.ParseData}.</li>
 * <li><tt>Status</tt>: From {@link org.apache.nutch.parse.ParseData}.</li>
 * <li><tt>Title</tt>: of the URL</li>
 * <li><tt>Outlinks</tt>: associated with the URL</li>
 * <li><tt>Content Metadata</tt>: such as <i>X-AspNet-Version</i>, <i>Date</i>,
 * <i>Content-length</i>, <i>servedBy</i>, <i>Content-Type</i>,
 * <i>Cache-Control</i>, etc.</li>
 * <li><tt>Parse Metadata</tt>: such as <i>CharEncodingForConversion</i>,
 * <i>OriginalCharEncoding</i>, <i>language</i>, etc.</li>
 * <li><tt>ParseText</tt>: The page parse text which varies in length depdnecing
 * on <code>content.length</code> configuration.</li>
 * </ol>
 * 
 */

public class ParserChecker extends AbstractChecker {

  protected URLNormalizers normalizers = null;
  protected boolean dumpText = false;
  protected boolean followRedirects = false;
  // used to simulate the metadata propagated from injection
  protected HashMap<String, String> metadata = new HashMap<>();
  protected String forceAsContentType = null;

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public int run(String[] args) throws Exception {
    String url = null;

    String usage = "Usage: ParserChecker [-normalize] [-followRedirects] [-dumpText] [-forceAs mimeType] [-md key=value] (-stdin | -listen <port> [-keepClientCnxOpen])";

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
      } else if (args[i].equals("-forceAs")) {
        forceAsContentType = args[++i];
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

    String contentType;
    if (forceAsContentType != null) {
      content.setContentType(forceAsContentType);
      contentType = forceAsContentType;
    } else {
      contentType = content.getContentType();
    }

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
      if (LOG.isWarnEnabled()) {
        LOG.warn("Couldn't pass score before parsing, url " + turl + " (" + e
            + ")");
        LOG.warn(StringUtils.stringifyException(e));
      }
    }

    ParseResult parseResult = new ParseUtil(getConf()).parse(content);

    if (parseResult == null) {
      LOG.error("Parsing content failed!");
      return (-1);
    }

    // calculate the signature
    byte[] signature = SignatureFactory.getSignature(getConf()).calculate(
        content, parseResult.get(new Text(url)));

    if (LOG.isInfoEnabled()) {
      LOG.info("parsing: " + url);
      LOG.info("contentType: " + contentType);
      LOG.info("signature: " + StringUtil.toHexString(signature));
    }

    Parse parse = parseResult.get(turl);
    if (parse == null) {
      LOG.error("Failed to get parse from parse result");
      LOG.error("Available parses in parse result (by URL key):");
      for (Map.Entry<Text, Parse> entry : parseResult) {
        LOG.error("  " + entry.getKey());
      }
      LOG.error("Parse result does not contain a parse for URL to be checked:");
      LOG.error("  " + turl);
      return -1;
    }

    // call the scoring filters
    try {
      scfilters.passScoreAfterParsing(turl, content, parse);
    } catch (Exception e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Couldn't pass score after parsing, url " + turl + " (" + e
            + ")");
        LOG.warn(StringUtils.stringifyException(e));
      }
    }

    for (Map.Entry<Text, Parse> entry : parseResult) {
      parse = entry.getValue();
      LOG.info("---------\nUrl\n---------------\n");
      System.out.print(entry.getKey());
      LOG.info("\n---------\nParseData\n---------\n");
      System.out.print(parse.getData().toString());
      if (dumpText) {
        LOG.info("---------\nParseText\n---------\n");
        System.out.print(parse.getText());
      }
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new ParserChecker(),
        args);
    System.exit(res);
  }

}

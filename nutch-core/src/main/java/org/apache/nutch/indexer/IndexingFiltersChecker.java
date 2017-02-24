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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.apache.nutch.net.URLNormalizers;
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

  protected URLNormalizers normalizers = null;
  protected boolean dumpText = false;
  protected boolean followRedirects = false;
  protected boolean keepClientCnxOpen = false;
  // used to simulate the metadata propagated from injection
  protected HashMap<String, String> metadata = new HashMap<>();
  protected int tcpPort = -1;

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public IndexingFiltersChecker() {

  }

  public int run(String[] args) throws Exception {
    String url = null;
    String usage = "Usage: IndexingFiltersChecker [-normalize] [-followRedirects] [-dumpText] [-md key=value] [-listen <port>] [-keepClientCnxOpen]";

    if (args.length == 0) {
      System.err.println(usage);
      return -1;
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-normalize")) {
        normalizers = new URLNormalizers(getConf(), URLNormalizers.SCOPE_DEFAULT);
      } else if (args[i].equals("-listen")) {
        tcpPort = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-followRedirects")) {
        followRedirects = true;
      } else if (args[i].equals("-keepClientCnxOpen")) {
        keepClientCnxOpen = true;
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
      } else if (i != args.length - 1) {
        System.err.println(usage);
        System.exit(-1);
      } else {
        url =args[i];
      }
    }
    
    // In listening mode?
    if (tcpPort == -1) {
      // No, just fetch and display
      StringBuilder output = new StringBuilder();
      int ret = fetch(url, output);
      System.out.println(output);
      return ret;
    } else {
      // Listen on socket and start workers on incoming requests
      listen();
    }
    
    return 0;
  }
  
  protected void listen() throws Exception {
    ServerSocket server = null;

    try{
      server = new ServerSocket();
      server.bind(new InetSocketAddress(tcpPort));
      LOG.info(server.toString());
    } catch (Exception e) {
      LOG.error("Could not listen on port " + tcpPort);
      System.exit(-1);
    }
    
    while(true){
      Worker worker;
      try{
        worker = new Worker(server.accept());
        Thread thread = new Thread(worker);
        thread.start();
      } catch (Exception e) {
        LOG.error("Accept failed: " + tcpPort);
        System.exit(-1);
      }
    }
  }
  
  private class Worker implements Runnable {
    private Socket client;

    Worker(Socket client) {
      this.client = client;
      LOG.info(client.toString());
    }

    public void run() {
      if (keepClientCnxOpen) {
        while (true) { // keep connection open until closes
          readWrite();
        }
      } else {
        readWrite();
        
        try { // close ourselves
          client.close();
        } catch (Exception e){
          LOG.error(e.toString());
        }
      }
    }
    
    protected void readWrite() {
      String line;
      BufferedReader in = null;
      PrintWriter out = null;
      
      try{
        in = new BufferedReader(new InputStreamReader(client.getInputStream()));
      } catch (Exception e) {
        LOG.error("in or out failed");
        System.exit(-1);
      }

      try{
        line = in.readLine();        
        StringBuilder output = new StringBuilder();
        fetch(line, output);
        
        client.getOutputStream().write(output.toString().getBytes(Charset.forName("UTF-8")));
      }catch (Exception e) {
        LOG.error("Read/Write failed: " + e);
      }
    }
  }
    
  
  protected int fetch(String url, StringBuilder output) throws Exception {
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

    IndexingFilters indexers = new IndexingFilters(getConf());
    
    int maxRedirects = 3;

    ProtocolOutput protocolOutput = getProtocolOutput(url, datum);
    Text turl = new Text(url);
    
    // Following redirects and not reached maxRedirects?
    while (!protocolOutput.getStatus().isSuccess() && followRedirects && protocolOutput.getStatus().isRedirect() && maxRedirects != 0) {
      String[] stuff = protocolOutput.getStatus().getArgs();
      url = stuff[0];
      
      if (normalizers != null) {
        url = normalizers.normalize(url, URLNormalizers.SCOPE_DEFAULT);
      }
    
      turl.set(url);
      
      // try again
      protocolOutput = getProtocolOutput(url, datum);
      maxRedirects--;
    }

    if (!protocolOutput.getStatus().isSuccess()) {
      output.append("Fetch failed with protocol status: "
          + protocolOutput.getStatus() + "\n");
      return 0;
    }

    Content content = protocolOutput.getContent();

    if (content == null) {
      output.append("No content for " + url + "\n");
      return 0;
    }

    String contentType = content.getContentType();

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
    datum.setSignature(signature);

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
      writers.open(new JobConf(getConf()), "IndexingFilterChecker");
      writers.write(doc);
      writers.close();
    }

    return 0;
  }
  
  protected ProtocolOutput getProtocolOutput(String url, CrawlDatum datum) throws Exception {
    ProtocolFactory factory = new ProtocolFactory(getConf());
    Protocol protocol = factory.getProtocol(url);
    Text turl = new Text(url);
    ProtocolOutput protocolOutput = protocol.getProtocolOutput(turl, datum);
    return protocolOutput;
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new IndexingFiltersChecker(), args);
    System.exit(res);
  }
}
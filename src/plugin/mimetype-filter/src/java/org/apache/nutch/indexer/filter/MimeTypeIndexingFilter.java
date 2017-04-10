/*
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

package org.apache.nutch.indexer.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.UnrecognizedOptionException;

// Nutch imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;

import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;

import org.apache.nutch.net.protocols.Response;

import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;

import org.apache.nutch.metadata.Metadata;

import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.PrefixStringMatcher;
import org.apache.nutch.util.TrieStringMatcher;
import org.apache.tika.Tika;

import java.lang.invoke.MethodHandles;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * An {@link org.apache.nutch.indexer.IndexingFilter} that allows filtering
 * of documents based on the MIME Type detected by Tika
 *
 */
public class MimeTypeIndexingFilter implements IndexingFilter {

  public static final String MIMEFILTER_REGEX_FILE = "mimetype.filter.file";

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private MimeUtil MIME;
  private Tika tika = new Tika();

  private TrieStringMatcher trie;

  private Configuration conf;

  private boolean acceptMode = true;

  // Inherited JavaDoc
  @Override
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {

    String mimeType;
    String contentType;

    Writable tcontentType = datum.getMetaData()
        .get(new Text(Response.CONTENT_TYPE));

    if (tcontentType != null) {
      contentType = tcontentType.toString();
    } else {
      contentType = parse.getData().getMeta(Response.CONTENT_TYPE);
    }

    if (contentType == null) {
      mimeType = tika.detect(url.toString());
    } else {
      mimeType = MIME.forName(MimeUtil.cleanMimeType(contentType));
    }

    contentType = mimeType;

    if (LOG.isInfoEnabled()) {
      LOG.info(String.format("[%s] %s", contentType, url));
    }

    if (trie != null) {
      if (trie.shortestMatch(contentType) == null) {
        // no match, but
        if (acceptMode) {
          return doc;
        }
        return null;
      } else {
        // matched, but we are blocking
        if (acceptMode) {
          return null;
        }
      }
    }

    return doc;
  }

  /*
   * -----------------------------
   * <implementation:Configurable> *
   * -----------------------------
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    MIME = new MimeUtil(conf);

    // load the file of the values
    String file = conf.get(MIMEFILTER_REGEX_FILE, "");

    if (file != null) {
      if (file.isEmpty()) {
        LOG.warn(String
            .format("Missing %s property, ALL mimetypes will be allowed",
                MIMEFILTER_REGEX_FILE));
      } else {
        Reader reader = conf.getConfResourceAsReader(file);

        try {
          readConfiguration(reader);
        } catch (IOException e) {
          if (LOG.isErrorEnabled()) {
            LOG.error(e.getMessage());
          }

          throw new RuntimeException(e.getMessage(), e);
        }
      }
    }
  }

  private void readConfiguration(Reader reader) throws IOException {
    BufferedReader in = new BufferedReader(reader);
    String line;
    List rules = new ArrayList();

    while (null != (line = in.readLine())) {
      if (line.length() == 0) {
        continue;
      }

      char first = line.charAt(0);
      switch (first) {
      case ' ':
      case '\n':
      case '#': // skip blank & comment lines
        break;
      case '+':
        acceptMode = true;
        break;
      case '-':
        acceptMode = false;
        break;
      default:
        rules.add(line);
        break;
      }
    }

    trie = new PrefixStringMatcher(rules);
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Main method for invoking this tool
   *
   * @throws IOException
   * @throws IndexingException
   */
  public static void main(String[] args) throws IOException, IndexingException {
    Option helpOpt = new Option("h", "help", false, "show this help message");
    Option rulesOpt = OptionBuilder.withArgName("file").hasArg()
        .withDescription(
            "Rules file to be used in the tests relative to the conf directory")
        .isRequired().create("rules");

    Options options = new Options();
    options.addOption(helpOpt).addOption(rulesOpt);

    CommandLineParser parser = new GnuParser();
    HelpFormatter formatter = new HelpFormatter();
    String rulesFile;

    try {
      CommandLine line = parser.parse(options, args);

      if (line.hasOption("help") || !line.hasOption("rules")) {
        formatter
            .printHelp("org.apache.nutch.indexer.filter.MimeTypeIndexingFilter",
                options, true);
        return;
      }

      rulesFile = line.getOptionValue("rules");
    } catch (UnrecognizedOptionException e) {
      formatter
          .printHelp("org.apache.nutch.indexer.filter.MimeTypeIndexingFilter",
              options, true);
      return;
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      e.printStackTrace();
      return;
    }

    MimeTypeIndexingFilter filter = new MimeTypeIndexingFilter();
    Configuration conf = NutchConfiguration.create();
    conf.set(MimeTypeIndexingFilter.MIMEFILTER_REGEX_FILE, rulesFile);
    filter.setConf(conf);

    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String line;

    while ((line = in.readLine()) != null && !line.isEmpty()) {
      Metadata metadata = new Metadata();
      metadata.set(Response.CONTENT_TYPE, line);
      ParseImpl parse = new ParseImpl("text",
          new ParseData(new ParseStatus(), "title", new Outlink[0], metadata));

      NutchDocument doc = filter.filter(new NutchDocument(), parse,
          new Text("http://www.example.com/"), new CrawlDatum(), new Inlinks());

      if (doc != null) {
        System.out.print("+ ");
        System.out.println(line);
      } else {
        System.out.print("- ");
        System.out.println(line);
      }
    }
  }
}

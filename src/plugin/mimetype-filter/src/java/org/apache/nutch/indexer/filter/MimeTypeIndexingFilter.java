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

// Nutch imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;

import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;

import org.apache.nutch.parse.Parse;
import org.apache.nutch.net.protocols.Response;

import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.PrefixStringMatcher;
import org.apache.nutch.util.TrieStringMatcher;
import org.apache.tika.Tika;

import java.io.BufferedReader;
import java.io.IOException;
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
      .getLogger(MimeTypeIndexingFilter.class);

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

    if (null != trie) {
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
    /*
     * ------------------------------ * </implementation:Configurable> *
     * ------------------------------
     */
}


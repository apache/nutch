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
package org.apache.nutch.indexer.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchIndexWriter;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class SolrWriter implements NutchIndexWriter {

  public static final Logger LOG = LoggerFactory.getLogger(SolrWriter.class);

  private SolrServer solr;
  private SolrMappingReader solrMapping;

  private final List<SolrInputDocument> inputDocs =
    new ArrayList<SolrInputDocument>();

  private int commitSize;

  @Override
  public void open(TaskAttemptContext job)
  throws IOException {
    Configuration conf = job.getConfiguration();
    solr = new CommonsHttpSolrServer(conf.get(SolrConstants.SERVER_URL));
    commitSize = conf.getInt(SolrConstants.COMMIT_SIZE, 1000);
    solrMapping = SolrMappingReader.getInstance(conf);
  }

  @Override
  public void write(NutchDocument doc) throws IOException {
    final SolrInputDocument inputDoc = new SolrInputDocument();
    for(final Entry<String, List<String>> e : doc) {
      for (final String val : e.getValue()) {

        Object val2 = val;
        if (e.getKey().equals("content")) {
          val2 = stripNonCharCodepoints((String)val);
        }

        inputDoc.addField(solrMapping.mapKey(e.getKey()), val2);
        String sCopy = solrMapping.mapCopyKey(e.getKey());
        if (sCopy != e.getKey()) {
        	inputDoc.addField(sCopy, val2);
        }
      }
    }
    inputDoc.setDocumentBoost(doc.getScore());
    inputDocs.add(inputDoc);
    if (inputDocs.size() >= commitSize) {
      try {
        LOG.info("Adding " + Integer.toString(inputDocs.size()) + " documents");
        solr.add(inputDocs);
      } catch (final SolrServerException e) {
        throw new IOException(e);
      }
      inputDocs.clear();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (!inputDocs.isEmpty()) {
        LOG.info("Adding " + Integer.toString(inputDocs.size()) + " documents");
        solr.add(inputDocs);
        inputDocs.clear();
      }
    } catch (final SolrServerException e) {
      throw new IOException(e);
    }
  }

  public static String stripNonCharCodepoints(String input) {
    StringBuilder retval = new StringBuilder();
    char ch;

    for (int i = 0; i < input.length(); i++) {
      ch = input.charAt(i);

      // Strip all non-characters http://unicode.org/cldr/utility/list-unicodeset.jsp?a=[:Noncharacter_Code_Point=True:]
      // and non-printable control characters except tabulator, new line and carriage return
      if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {

        retval.append(ch);
      }
    }

    return retval.toString();
  }

}

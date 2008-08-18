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

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.nutch.analysis.NutchDocumentAnalyzer;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class TestIndexSorter extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestIndexSorter.class);
  
  private static final String INDEX_PLAIN = "index";
  private static final String INDEX_SORTED = "index-sorted";
  private static final int NUM_DOCS = 254;
  private String[] fieldNames = new String[] {
      "id",
      "url",
      "site",
      "content",
      "host",
      "anchor",
      "boost"
  };
  
  Configuration conf = null;
  File testDir = null;
  Directory dir = null;
  
  
  protected void setUp() throws Exception {
    if (conf == null) conf = NutchConfiguration.create();
    // create test index
    testDir = new File("indexSorter-test-" + System.currentTimeMillis());
    if (!testDir.mkdirs()) {
      throw new Exception("Can't create test dir " + testDir.toString());
    }
    LOG.info("Creating test index: " + testDir.getAbsolutePath());
    File plain = new File(testDir, INDEX_PLAIN);
    Directory dir = FSDirectory.getDirectory(plain);
    IndexWriter writer = new IndexWriter(dir, new NutchDocumentAnalyzer(conf), true);
    // create test documents
    for (int i = 0; i < NUM_DOCS; i++) {
      Document doc = new Document();
      for (int k = 0; k < fieldNames.length; k++) {
        Field f;
        Store s;
        Index ix;
        String val = null;
        if (fieldNames[k].equals("id")) {
          s = Store.YES;
          ix = Index.UN_TOKENIZED;
          val = String.valueOf(i);
        } else if (fieldNames[k].equals("host")) {
          s = Store.YES;
          ix = Index.UN_TOKENIZED;
          val = "www.example" + i + ".com";
        } else if (fieldNames[k].equals("site")) {
          s = Store.NO;
          ix = Index.UN_TOKENIZED;
          val = "www.example" + i + ".com";
        } else if (fieldNames[k].equals("content")) {
          s = Store.NO;
          ix = Index.TOKENIZED;
          val = "This is the content of the " + i + "-th document.";
        } else if (fieldNames[k].equals("boost")) {
          s = Store.YES;
          ix = Index.NO;
          // XXX note that this way we ensure different values of encoded boost
          // XXX note also that for this reason we can't reliably test more than
          // XXX 255 documents.
          float boost = Similarity.decodeNorm((byte)(i + 1));
          val = String.valueOf(boost);
          doc.setBoost(boost);
        } else {
          s = Store.YES;
          ix = Index.TOKENIZED;
          if (fieldNames[k].equals("anchor")) {
            val = "anchors to " + i + "-th page.";
          } else if (fieldNames[k].equals("url")) {
            val = "http://www.example" + i + ".com/" + i + ".html";
          }
        }
        f = new Field(fieldNames[k], val, s, ix);
        doc.add(f);
      }
      writer.addDocument(doc);
    }
    writer.optimize();
    writer.close();
  }
  
  protected void tearDown() throws Exception {
    FileUtil.fullyDelete(testDir);
  }
  
  public void testSorting() throws Exception {
    IndexSorter sorter = new IndexSorter(conf);
    sorter.sort(testDir);
    // read back documents
    IndexReader reader = IndexReader.open(new File(testDir, INDEX_SORTED));
    assertEquals(reader.numDocs(), NUM_DOCS);
    for (int i = 0; i < reader.maxDoc(); i++) {
      Document doc = reader.document(i);
      Field f = doc.getField("content");
      assertNull(f);
      f = doc.getField("boost");
      float boost = Similarity.decodeNorm((byte)(NUM_DOCS - i));
      String cmp = String.valueOf(boost);
      assertEquals(cmp, f.stringValue());
    }
    reader.close();
  }

}

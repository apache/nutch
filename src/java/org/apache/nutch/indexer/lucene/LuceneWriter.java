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
package org.apache.nutch.indexer.lucene;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.FSDirectory;
import org.apache.nutch.analysis.AnalyzerFactory;
import org.apache.nutch.analysis.NutchAnalyzer;
import org.apache.nutch.analysis.NutchDocumentAnalyzer;
import org.apache.nutch.indexer.Indexer;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchIndexWriter;
import org.apache.nutch.indexer.NutchSimilarity;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.util.LogUtil;

public class LuceneWriter implements NutchIndexWriter {

  public static enum STORE { YES, NO, COMPRESS }

  public static enum INDEX { NO, NO_NORMS, TOKENIZED, UNTOKENIZED }

  public static enum VECTOR { NO, OFFSET, POS, POS_OFFSET, YES }

  private IndexWriter writer;

  private AnalyzerFactory analyzerFactory;

  private Path perm;

  private Path temp;

  private FileSystem fs;

  private final Map<String, Field.Store> fieldStore;

  private final Map<String, Field.Index> fieldIndex;

  private final Map<String, Field.TermVector> fieldVector;

  public LuceneWriter() {
    fieldStore = new HashMap<String, Field.Store>();
    fieldIndex = new HashMap<String, Field.Index>();
    fieldVector = new HashMap<String, Field.TermVector>();
  }

  private Document createLuceneDoc(NutchDocument doc) {
    final Document out = new Document();

    out.setBoost(doc.getScore());

    final Metadata documentMeta = doc.getDocumentMeta();
    for (final Entry<String, List<String>> entry : doc) {
      final String fieldName = entry.getKey();

      Field.Store store = fieldStore.get(fieldName);
      Field.Index index = fieldIndex.get(fieldName);
      Field.TermVector vector = fieldVector.get(fieldName);

      // default values
      if (store == null) {
        store = Field.Store.NO;
      }

      if (index == null) {
        index = Field.Index.NO;
      }

      if (vector == null) {
        vector = Field.TermVector.NO;
      }

      // read document-level field information
      final String[] fieldMetas =
        documentMeta.getValues(LuceneConstants.FIELD_PREFIX + fieldName);
      if (fieldMetas.length != 0) {
        for (final String val : fieldMetas) {
          if (LuceneConstants.STORE_YES.equals(val)) {
            store = Field.Store.YES;
          } else if (LuceneConstants.STORE_NO.equals(val)) {
            store = Field.Store.NO;
          } else if (LuceneConstants.INDEX_TOKENIZED.equals(val)) {
            index = Field.Index.ANALYZED;
          } else if (LuceneConstants.INDEX_NO.equals(val)) {
            index = Field.Index.NO;
          } else if (LuceneConstants.INDEX_UNTOKENIZED.equals(val)) {
            index = Field.Index.NOT_ANALYZED;
          } else if (LuceneConstants.INDEX_NO_NORMS.equals(val)) {
            index = Field.Index.ANALYZED_NO_NORMS;
          } else if (LuceneConstants.VECTOR_NO.equals(val)) {
            vector = Field.TermVector.NO;
          } else if (LuceneConstants.VECTOR_YES.equals(val)) {
            vector = Field.TermVector.YES;
          } else if (LuceneConstants.VECTOR_POS.equals(val)) {
            vector = Field.TermVector.WITH_POSITIONS;
          } else if (LuceneConstants.VECTOR_POS_OFFSET.equals(val)) {
            vector = Field.TermVector.WITH_POSITIONS_OFFSETS;
          } else if (LuceneConstants.VECTOR_OFFSET.equals(val)) {
            vector = Field.TermVector.WITH_OFFSETS;
          }
        }
      }

      for (final String fieldValue : entry.getValue()) {
        out.add(new Field(fieldName, fieldValue, store, index, vector));
      }
    }

    return out;
  }

  @SuppressWarnings("unchecked")
  private void processOptions(Configuration conf) {
    final Iterator iterator = conf.iterator();
    while (iterator.hasNext()) {
      final String key = (String) ((Map.Entry)iterator.next()).getKey();
      if (!key.startsWith(LuceneConstants.LUCENE_PREFIX)) {
        continue;
      }
      if (key.startsWith(LuceneConstants.FIELD_STORE_PREFIX)) {
        final String field =
          key.substring(LuceneConstants.FIELD_STORE_PREFIX.length());
        final LuceneWriter.STORE store = LuceneWriter.STORE.valueOf(conf.get(key));
        switch (store) {
        case YES:
        case COMPRESS:
          fieldStore.put(field, Field.Store.YES);
          break;
        case NO:
          fieldStore.put(field, Field.Store.NO);
          break;
        }
      } else if (key.startsWith(LuceneConstants.FIELD_INDEX_PREFIX)) {
        final String field =
          key.substring(LuceneConstants.FIELD_INDEX_PREFIX.length());
        final LuceneWriter.INDEX index = LuceneWriter.INDEX.valueOf(conf.get(key));
        switch (index) {
        case NO:
          fieldIndex.put(field, Field.Index.NO);
          break;
        case NO_NORMS:
          fieldIndex.put(field, Field.Index.NOT_ANALYZED_NO_NORMS);
          break;
        case TOKENIZED:
          fieldIndex.put(field, Field.Index.ANALYZED);
          break;
        case UNTOKENIZED:
          fieldIndex.put(field, Field.Index.NOT_ANALYZED);
          break;
        }
      } else if (key.startsWith(LuceneConstants.FIELD_VECTOR_PREFIX)) {
        final String field =
          key.substring(LuceneConstants.FIELD_VECTOR_PREFIX.length());
        final LuceneWriter.VECTOR vector = LuceneWriter.VECTOR.valueOf(conf.get(key));
        switch (vector) {
        case NO:
          fieldVector.put(field, Field.TermVector.NO);
          break;
        case OFFSET:
          fieldVector.put(field, Field.TermVector.WITH_OFFSETS);
          break;
        case POS:
          fieldVector.put(field, Field.TermVector.WITH_POSITIONS);
          break;
        case POS_OFFSET:
          fieldVector.put(field, Field.TermVector.WITH_POSITIONS_OFFSETS);
          break;
        case YES:
          fieldVector.put(field, Field.TermVector.YES);
          break;
        }
      }
    }
  }

  public void open(JobConf job, String name)
  throws IOException {
    this.fs = FileSystem.get(job);
    perm = new Path(FileOutputFormat.getOutputPath(job), name);
    temp = job.getLocalPath("index/_"  +
                      Integer.toString(new Random().nextInt()));

    fs.delete(perm, true); // delete old, if any
    analyzerFactory = new AnalyzerFactory(job);
    writer = new IndexWriter(
        FSDirectory.open(new File(fs.startLocalOutput(perm, temp).toString())),
        new NutchDocumentAnalyzer(job), true, MaxFieldLength.UNLIMITED);

    writer.setMergeFactor(job.getInt("indexer.mergeFactor", 10));
    writer.setMaxBufferedDocs(job.getInt("indexer.minMergeDocs", 100));
    writer.setMaxMergeDocs(job
        .getInt("indexer.maxMergeDocs", Integer.MAX_VALUE));
    writer.setTermIndexInterval(job.getInt("indexer.termIndexInterval", 128));
    writer.setMaxFieldLength(job.getInt("indexer.max.tokens", 10000));
    writer.setInfoStream(LogUtil.getDebugStream(Indexer.LOG));
    writer.setUseCompoundFile(false);
    writer.setSimilarity(new NutchSimilarity());

    processOptions(job);
  }

  public void close() throws IOException {
    writer.optimize();
    writer.close();
    fs.completeLocalOutput(perm, temp); // copy to dfs
    fs.createNewFile(new Path(perm, Indexer.DONE_NAME));
  }

  public void write(NutchDocument doc) throws IOException {
    final Document luceneDoc = createLuceneDoc(doc);
    final NutchAnalyzer analyzer = analyzerFactory.get(luceneDoc.get("lang"));
    if (Indexer.LOG.isDebugEnabled()) {
      Indexer.LOG.debug("Indexing [" + luceneDoc.get("url")
          + "] with analyzer " + analyzer + " (" + luceneDoc.get("lang")
          + ")");
    }
    writer.addDocument(luceneDoc, analyzer);

  }

  /** Adds a lucene field.
   * <p>
   * This method is provided for backward-compatibility with
   * older indexing filters. This should not be used by newer
   * implementations since this is slower than
   * {@link NutchDocument#add(String, String)} and will be removed
   * in a future release.
   * </p>
   * @param f Lucene field to be added.
   * @deprecated Use {@link NutchDocument#add(String, String)} instead and
   * set index-level metadata for field information.
   * */
  @Deprecated
  public static void add(NutchDocument doc, Field f) {
    final String fieldName = f.name();
    final String key = LuceneConstants.FIELD_PREFIX + fieldName;
    final Metadata documentMeta = doc.getDocumentMeta();
    if (f.isStored()) {
      documentMeta.add(key, LuceneConstants.STORE_YES);
    } else {
      documentMeta.add(key, LuceneConstants.STORE_NO);
    }

    if (f.isIndexed()) {
      if (f.isTokenized()) {
        documentMeta.add(key, LuceneConstants.INDEX_TOKENIZED);
      } else if (f.getOmitNorms()) {
        documentMeta.add(key, LuceneConstants.INDEX_NO_NORMS);
      } else {
        documentMeta.add(key, LuceneConstants.INDEX_UNTOKENIZED);
      }
    } else {
      documentMeta.add(key, LuceneConstants.INDEX_NO);
    }

    if (f.isStoreOffsetWithTermVector() && f.isStorePositionWithTermVector()) {
      documentMeta.add(key, LuceneConstants.VECTOR_POS_OFFSET);
    } else if (f.isStoreOffsetWithTermVector()) {
      documentMeta.add(key, LuceneConstants.VECTOR_OFFSET);
    } else if (f.isStorePositionWithTermVector()) {
      documentMeta.add(key, LuceneConstants.VECTOR_POS);
    } else if (f.isTermVectorStored()) {
      documentMeta.add(key, LuceneConstants.VECTOR_YES);
    } else {
      documentMeta.add(key, LuceneConstants.VECTOR_NO);
    }
  }

  public static void addFieldOptions(String field, LuceneWriter.STORE store,
      LuceneWriter.INDEX index, LuceneWriter.VECTOR vector, Configuration conf) {

    /* Only set the field options if none have been configured already */
    if (conf.get(LuceneConstants.FIELD_STORE_PREFIX + field) == null) {
    	conf.set(LuceneConstants.FIELD_STORE_PREFIX + field, store.toString());
    }
    if (conf.get(LuceneConstants.FIELD_INDEX_PREFIX + field) == null) {
	    conf.set(LuceneConstants.FIELD_INDEX_PREFIX + field, index.toString());
    }
    if (conf.get(LuceneConstants.FIELD_VECTOR_PREFIX + field) == null) {
	    conf.set(LuceneConstants.FIELD_VECTOR_PREFIX + field, vector.toString());
    }
  }

  public static void addFieldOptions(String field, LuceneWriter.STORE store,
      LuceneWriter.INDEX index, Configuration conf) {
    LuceneWriter.addFieldOptions(field, store, index, LuceneWriter.VECTOR.NO, conf);
  }
}

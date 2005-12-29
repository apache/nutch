/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.searcher;

import java.io.IOException;
import java.io.File;

import java.util.ArrayList;
import java.util.Enumeration;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;

import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FieldCache;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.nutch.fs.*;
import org.apache.nutch.io.*;
import org.apache.nutch.util.*;
import org.apache.nutch.indexer.*;

/** Implements {@link Searcher} and {@link HitDetailer} for either a single
 * merged index, or a set of indexes. */
public class IndexSearcher implements Searcher, HitDetailer {

  private org.apache.lucene.search.Searcher luceneSearcher;
  private org.apache.lucene.index.IndexReader reader;

  private LuceneQueryOptimizer optimizer = new LuceneQueryOptimizer
    (NutchConf.get().getInt("searcher.filter.cache.size", 16),
     NutchConf.get().getFloat("searcher.filter.cache.threshold", 0.05f));

  /** Construct given a number of indexes. */
  public IndexSearcher(File[] indexDirs) throws IOException {
    IndexReader[] readers = new IndexReader[indexDirs.length];
    for (int i = 0; i < indexDirs.length; i++) {
      readers[i] = IndexReader.open(getDirectory(indexDirs[i]));
    }
    init(new MultiReader(readers));
  }

  /** Construct given a single merged index. */
  public IndexSearcher(File index)
    throws IOException {
    init(IndexReader.open(getDirectory(index)));
  }

  private void init(IndexReader reader) throws IOException {
    this.reader = reader;
    this.luceneSearcher = new org.apache.lucene.search.IndexSearcher(reader);
    this.luceneSearcher.setSimilarity(new NutchSimilarity());
  }

  private Directory getDirectory(File file) throws IOException {
    NutchFileSystem fs = NutchFileSystem.get();
    if ("local".equals(fs.getName())) {
      return FSDirectory.getDirectory(file, false);
    } else {
      return new NdfsDirectory(fs, file, false);
    }
  }

  public Hits search(Query query, int numHits,
                     String dedupField, String sortField, boolean reverse)

    throws IOException {

    org.apache.lucene.search.BooleanQuery luceneQuery =
      QueryFilters.filter(query);
    
    return translateHits
      (optimizer.optimize(luceneQuery, luceneSearcher, numHits,
                          sortField, reverse),
       dedupField, sortField);
  }

  public String getExplanation(Query query, Hit hit) throws IOException {
    return luceneSearcher.explain(QueryFilters.filter(query),
                                  hit.getIndexDocNo()).toHtml();
  }

  public HitDetails getDetails(Hit hit) throws IOException {
    ArrayList fields = new ArrayList();
    ArrayList values = new ArrayList();

    Document doc = luceneSearcher.doc(hit.getIndexDocNo());

    Enumeration e = doc.fields();
    while (e.hasMoreElements()) {
      Field field = (Field)e.nextElement();
      fields.add(field.name());
      values.add(field.stringValue());
    }

    return new HitDetails((String[])fields.toArray(new String[fields.size()]),
                          (String[])values.toArray(new String[values.size()]));
  }

  public HitDetails[] getDetails(Hit[] hits) throws IOException {
    HitDetails[] results = new HitDetails[hits.length];
    for (int i = 0; i < hits.length; i++)
      results[i] = getDetails(hits[i]);
    return results;
  }

  private Hits translateHits(TopDocs topDocs,
                             String dedupField, String sortField)
    throws IOException {

    String[] dedupValues = null;
    if (dedupField != null) 
      dedupValues = FieldCache.DEFAULT.getStrings(reader, dedupField);

    ScoreDoc[] scoreDocs = topDocs.scoreDocs;
    int length = scoreDocs.length;
    Hit[] hits = new Hit[length];
    for (int i = 0; i < length; i++) {
      
      int doc = scoreDocs[i].doc;
      
      WritableComparable sortValue;               // convert value to writable
      if (sortField == null) {
        sortValue = new FloatWritable(scoreDocs[i].score);
      } else {
        Object raw = ((FieldDoc)scoreDocs[i]).fields[0];
        if (raw instanceof Integer) {
          sortValue = new IntWritable(((Integer)raw).intValue());
        } else if (raw instanceof Float) {
          sortValue = new FloatWritable(((Float)raw).floatValue());
        } else if (raw instanceof String) {
          sortValue = new UTF8((String)raw);
        } else {
          throw new RuntimeException("Unknown sort value type!");
        }
      }

      String dedupValue = dedupValues == null ? null : dedupValues[doc];

      hits[i] = new Hit(doc, sortValue, dedupValue);
    }
    return new Hits(topDocs.totalHits, hits);
  }

}

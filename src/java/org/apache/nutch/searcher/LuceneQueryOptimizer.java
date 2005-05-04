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

import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.QueryFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Sort;

import java.util.LinkedHashMap;
import java.util.Map;
import java.io.IOException;

/** Utility which converts certain query clauses into {@link QueryFilter}s and
 * caches these.  Only required {@link TermQuery}s whose boost is zero and
 * whose term occurs in at least a certain fraction of documents are converted
 * to cached filters.  This accellerates query constraints like language,
 * document format, etc., which do not affect ranking but might otherwise slow
 * search considerably. */
class LuceneQueryOptimizer {
  private LinkedHashMap cache;                   // an LRU cache of QueryFilter

  private float threshold;

  /** Construct an optimizer that caches and uses filters for required {@link
   * TermQuery}s whose boost is zero.
   * @param cacheSize the number of QueryFilters to cache
   * @param threshold the fraction of documents which must contain term
   */
  public LuceneQueryOptimizer(final int cacheSize, float threshold) {
    this.cache = new LinkedHashMap(cacheSize, 0.75f, true) {
        protected boolean removeEldestEntry(Map.Entry eldest) {
          return size() > cacheSize;              // limit size of cache
        }
      };
    this.threshold = threshold;
  }

  public TopDocs optimize(BooleanQuery original,
                          Searcher searcher, int numHits,
                          String sortField, boolean reverse)
    throws IOException {

    BooleanQuery query = new BooleanQuery();
    BooleanQuery filterQuery = null;

    BooleanClause[] clauses = original.getClauses();
    for (int i = 0; i < clauses.length; i++) {
      BooleanClause c = clauses[i];
      if (c.required                              // required
          && c.query.getBoost() == 0.0f           // boost is zero
          && c.query instanceof TermQuery         // TermQuery
          && (searcher.docFreq(((TermQuery)c.query).getTerm())
              / (float)searcher.maxDoc()) >= threshold) { // check threshold
        if (filterQuery == null)
          filterQuery = new BooleanQuery();
        filterQuery.add(c.query, true, false);    // filter it
      } else {
        query.add(c);                             // query it
      }
    }

    Filter filter = null;
    if (filterQuery != null) {
      synchronized (cache) {                      // check cache
        filter = (Filter)cache.get(filterQuery);
      }
      if (filter == null) {                       // miss
        filter = new QueryFilter(filterQuery);    // construct new entry
        synchronized (cache) {
          cache.put(filterQuery, filter);         // cache it
        }
      }        
    }

    if (sortField == null && !reverse) {
      return searcher.search(query, filter, numHits);
    } else {
      return searcher.search(query, filter, numHits,
                             new Sort(sortField, reverse));
    }
  }
}

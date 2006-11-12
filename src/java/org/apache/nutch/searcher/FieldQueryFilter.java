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

package org.apache.nutch.searcher;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.index.Term;

import org.apache.nutch.analysis.CommonGrams;

import org.apache.nutch.searcher.Query.Clause;
import org.apache.nutch.searcher.Query.Phrase;
import org.apache.hadoop.conf.Configuration;

/** Translate query fields to search the same-named field, as indexed by an
 * IndexingFilter.  Best for tokenized fields. */
public abstract class FieldQueryFilter implements QueryFilter {
  private String field;
  private float boost = 1.0f;
  private Configuration conf;
  private CommonGrams commonGrams;

  /** Construct for the named field.*/
  protected FieldQueryFilter(String field) {
    this(field, 1.0f);
  }

  /** Construct for the named field, boosting as specified.*/
  protected FieldQueryFilter(String field, float boost) {
    this.field = field;
    this.boost = boost;
  }

  public BooleanQuery filter(Query input, BooleanQuery output)
    throws QueryException {
    
    // examine each clause in the Nutch query
    Clause[] clauses = input.getClauses();
    for (int i = 0; i < clauses.length; i++) {
      Clause c = clauses[i];

      // skip non-matching clauses
      if (!c.getField().equals(field))
        continue;

      // optimize phrase clause
      if (c.isPhrase()) {
        String[] opt = this.commonGrams.optimizePhrase(c.getPhrase(), field);
        if (opt.length==1) {
          c = new Clause(new Query.Term(opt[0]),
                         c.isRequired(), c.isProhibited(), getConf());
        } else {
          c = new Clause(new Phrase(opt), c.isRequired(), c.isProhibited(), getConf());
        }
      }

      // construct appropriate Lucene clause
      org.apache.lucene.search.Query luceneClause;
      if (c.isPhrase()) {
        Phrase nutchPhrase = c.getPhrase();
        Query.Term[] terms = nutchPhrase.getTerms();
        PhraseQuery lucenePhrase = new PhraseQuery();
        for (int j = 0; j < terms.length; j++) {
          lucenePhrase.add(new Term(field, terms[j].toString()));
        }
        luceneClause = lucenePhrase;
      } else {
        luceneClause = new TermQuery(new Term(field, c.getTerm().toString()));
      }

      // set boost
      luceneClause.setBoost(boost);
      // add it as specified in query
      
      output.add(luceneClause, 
          (c.isProhibited()
              ? BooleanClause.Occur.MUST_NOT
              : (c.isRequired()
                  ? BooleanClause.Occur.MUST
                  : BooleanClause.Occur.SHOULD
                 )
           ));
    }
    
    // return the modified Lucene query
    return output;
  }
  
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.commonGrams = new CommonGrams(conf);
  }

  public Configuration getConf() {
    return this.conf;
  }
}

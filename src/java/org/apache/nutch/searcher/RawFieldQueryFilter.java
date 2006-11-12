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
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.index.Term;

import org.apache.nutch.searcher.Query.Clause;

/** Translate raw query fields to search the same-named field, as indexed by an
 * IndexingFilter. */
public abstract class RawFieldQueryFilter implements QueryFilter {
  private String field;
  private boolean lowerCase;
  private float boost;

  /** Construct for the named field, lowercasing query values.*/
  protected RawFieldQueryFilter(String field) {
    this(field, true);
  }

  /** Construct for the named field, lowercasing query values.*/
  protected RawFieldQueryFilter(String field, float boost) {
    this(field, true, boost);
  }

  /** Construct for the named field, potentially lowercasing query values.*/
  protected RawFieldQueryFilter(String field, boolean lowerCase) {
    this(field, lowerCase, 0.0f);
  }

  /** Construct for the named field, potentially lowercasing query values.*/
  protected RawFieldQueryFilter(String field, boolean lowerCase, float boost) {
    this.field = field;
    this.lowerCase = lowerCase;
    this.boost = boost;
  }

  protected void setBoost(float boost) {
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

      // get the field value from the clause
      // raw fields are guaranteed to be Terms, not Phrases
      String value = c.getTerm().toString();
      if (lowerCase)
        value = value.toLowerCase();

      // add a Lucene TermQuery for this clause
      TermQuery clause = new TermQuery(new Term(field, value));
      // set boost
      clause.setBoost(boost);
      // add it as specified in query
      
      output.add(clause, 
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
}

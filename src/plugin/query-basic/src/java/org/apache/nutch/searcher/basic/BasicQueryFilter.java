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

package org.apache.nutch.searcher.basic;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermQuery;

import org.apache.nutch.analysis.NutchDocumentAnalyzer;
import org.apache.nutch.analysis.CommonGrams;

import org.apache.nutch.searcher.QueryFilter;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.searcher.Query.*;
import org.apache.hadoop.conf.Configuration;

/** The default query filter.  Query terms in the default query field are
 * expanded to search the url, anchor and content document fields.*/
public class BasicQueryFilter implements QueryFilter {
    
  private float URL_BOOST;

  private float ANCHOR_BOOST ;

  private float TITLE_BOOST;

  private float HOST_BOOST;

  private static int SLOP = Integer.MAX_VALUE;

  private float PHRASE_BOOST;

  private static final String[] FIELDS =
  { "url", "anchor", "content", "title", "host" };

  private final float[] FIELD_BOOSTS =
  { URL_BOOST, ANCHOR_BOOST, 1.0f, TITLE_BOOST, HOST_BOOST };

  /**
   * Set the boost factor for url matches, relative to content and anchor
   * matches
   */
  public void setUrlBoost(float boost) { URL_BOOST = boost; }

  /** Set the boost factor for title/anchor matches, relative to url and
   * content matches. */
  public void setAnchorBoost(float boost) { ANCHOR_BOOST = boost; }

  /** Set the boost factor for sloppy phrase matches relative to unordered term
   * matches. */
  public void setPhraseBoost(float boost) { PHRASE_BOOST = boost; }

  /** Set the maximum number of terms permitted between matching terms in a
   * sloppy phrase match. */
  public void setSlop(int slop) { SLOP = slop; }

  private Configuration conf;

  public BooleanQuery filter(Query input, BooleanQuery output) {
    addTerms(input, output);
    addSloppyPhrases(input, output);
    return output;
  }

  private void addTerms(Query input, BooleanQuery output) {
    Clause[] clauses = input.getClauses();
    for (int i = 0; i < clauses.length; i++) {
      Clause c = clauses[i];

      if (!c.getField().equals(Clause.DEFAULT_FIELD))
        continue;                                 // skip non-default fields

      BooleanQuery out = new BooleanQuery();
      for (int f = 0; f < FIELDS.length; f++) {

        Clause o = c;
        if (c.isPhrase()) {                         // optimize phrase clauses
          String[] opt = new CommonGrams(getConf()).optimizePhrase(c.getPhrase(), FIELDS[f]);
          if (opt.length==1) {
            o = new Clause(new Term(opt[0]), c.isRequired(), c.isProhibited(), getConf());
          } else {
            o = new Clause(new Phrase(opt), c.isRequired(), c.isProhibited(), getConf());
          }
        }

        out.add(o.isPhrase()
                ? exactPhrase(o.getPhrase(), FIELDS[f], FIELD_BOOSTS[f])
                : termQuery(FIELDS[f], o.getTerm(), FIELD_BOOSTS[f]),
                BooleanClause.Occur.SHOULD);
      }
      output.add(out, (c.isProhibited()
              ? BooleanClause.Occur.MUST_NOT
              : (c.isRequired()
                  ? BooleanClause.Occur.MUST
                  : BooleanClause.Occur.SHOULD
                )));
    }
  }

  private void addSloppyPhrases(Query input, BooleanQuery output) {
    Clause[] clauses = input.getClauses();
    for (int f = 0; f < FIELDS.length; f++) {

      PhraseQuery sloppyPhrase = new PhraseQuery();
      sloppyPhrase.setBoost(FIELD_BOOSTS[f] * PHRASE_BOOST);
      sloppyPhrase.setSlop("anchor".equals(FIELDS[f])
                           ? NutchDocumentAnalyzer.INTER_ANCHOR_GAP
                           : SLOP);
      int sloppyTerms = 0;

      for (int i = 0; i < clauses.length; i++) {
        Clause c = clauses[i];
        
        if (!c.getField().equals(Clause.DEFAULT_FIELD))
          continue;                               // skip non-default fields
        
        if (c.isPhrase())                         // skip exact phrases
          continue;

        if (c.isProhibited())                     // skip prohibited terms
          continue;
        
        sloppyPhrase.add(luceneTerm(FIELDS[f], c.getTerm()));
        sloppyTerms++;
      }

      if (sloppyTerms > 1)
        output.add(sloppyPhrase, BooleanClause.Occur.SHOULD);
    }
  }


  private org.apache.lucene.search.Query
        termQuery(String field, Term term, float boost) {
    TermQuery result = new TermQuery(luceneTerm(field, term));
    result.setBoost(boost);
    return result;
  }

  /** Utility to construct a Lucene exact phrase query for a Nutch phrase. */
  private org.apache.lucene.search.Query
       exactPhrase(Phrase nutchPhrase,
                   String field, float boost) {
    Term[] terms = nutchPhrase.getTerms();
    PhraseQuery exactPhrase = new PhraseQuery();
    for (int i = 0; i < terms.length; i++) {
      exactPhrase.add(luceneTerm(field, terms[i]));
    }
    exactPhrase.setBoost(boost);
    return exactPhrase;
  }

  /** Utility to construct a Lucene Term given a Nutch query term and field. */
  private static org.apache.lucene.index.Term luceneTerm(String field,
                                                         Term term) {
    return new org.apache.lucene.index.Term(field, term.toString());
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.URL_BOOST = conf.getFloat("query.url.boost", 4.0f);
    this.ANCHOR_BOOST = conf.getFloat("query.anchor.boost", 2.0f);
    this.TITLE_BOOST = conf.getFloat("query.title.boost", 1.5f);
    this.HOST_BOOST = conf.getFloat("query.host.boost", 2.0f);
    this.PHRASE_BOOST = conf.getFloat("query.phrase.boost", 1.0f);
  }

  public Configuration getConf() {
    return this.conf;
  }
}

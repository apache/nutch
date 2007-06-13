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
package org.apache.nutch.spell;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.lucene.search.IndexSearcher;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;

/**
 * Parses queries and sends them to NGramSpeller for spell checking.
 * 
 * @author Andy Liu &lt;andyliu1227@gmail.com&gt
 */
public class SpellCheckerBean {
  public static final Log LOG = LogFactory.getLog(SpellCheckerBean.class);

  IndexSearcher spellingSearcher;

  //
  // Configuration parameters used by NGramSpeller . Hardcoded for now.
  //	
  final int minThreshold = 5;

  final int ng1 = 3;

  final int ng2 = 4;

  final int maxr = 10;

  final int maxd = 5;

  final float bStart = 2.0f;

  final float bEnd = 1.0f;

  final float bTransposition = 6.5f;

  // configuration variable names
  public static final String SPELLING_INDEX_LOCATION = "spell.index.dir";

  public static final String SPELLING_DOCFREQ_THRESHOLD = "spell.docfreq.threshold";

  public static final String SPELLING_DOCFREQ_THRESHOLD_FACTOR = "spell.docfreq.threshold.factor";

  String indexLocation;

  int threshold;

  int thresholdFactor;
  
  Configuration conf;

  public SpellCheckerBean(Configuration conf) {
    this.conf=conf;
    indexLocation = conf.get(SPELLING_INDEX_LOCATION, "./spelling");
    threshold = conf.getInt(SPELLING_DOCFREQ_THRESHOLD, 100);
    thresholdFactor = conf.getInt(SPELLING_DOCFREQ_THRESHOLD_FACTOR, 10);
    try {
      spellingSearcher = new IndexSearcher(indexLocation);
    } catch (IOException ioe) {
      LOG.info("error opening spell checking index");
      ioe.printStackTrace(LogUtil.getInfoStream(LOG));
    }
  }

  /** Cache in Configuration. */
  public static SpellCheckerBean get(Configuration conf) {
    SpellCheckerBean spellCheckerBean = (SpellCheckerBean) conf
        .getObject(SpellCheckerBean.class.getName());

    if (spellCheckerBean == null) {
      LOG.info("creating new spell checker bean");
      spellCheckerBean = new SpellCheckerBean(conf);
      conf.setObject(SpellCheckerBean.class.getName(), spellCheckerBean);
    }
    return spellCheckerBean;
  }

  public SpellCheckerTerms checkSpelling(Query query, String queryString) {

    return checkSpelling(query, queryString, threshold, thresholdFactor);
  }

  /**
   * Parses original query, retrieves suggestions from ngrams spelling index
   * 
   * @param originalQuery
   *          Query to be spell-checked
   * @param docFreqThreshold
   *          Terms in query that have a docFreq lower than this threshold
   *          qualify as "mispelled"
   * @param factorThreshold
   *          The suggested term must have a docFreq at least factorThreshold
   *          times more than the mispelled term. Set to 1 to disable.
   * @return terms with corrected spelling
   */
  public SpellCheckerTerms checkSpelling(Query query, String queryString,
      int docFreqThreshold, int factorThreshold) {
    SpellCheckerTerms spellCheckerTerms = null;

    try {
      spellCheckerTerms = parseOriginalQuery(query, queryString);

      for (int i = 0; i < spellCheckerTerms.size(); i++) {
        SpellCheckerTerm currentTerm = spellCheckerTerms.getSpellCheckerTerm(i);
        String originalTerm = currentTerm.getOriginalTerm();

        spellCheckerTerms.getSpellCheckerTerm(i).setOriginalDocFreq(
            getDocFreq(originalTerm));

        //
        // Spell checking is not effective for words under 4 letters long
        // Any words over 25 letters long isn't worth checking either.
        //
        if (originalTerm.length() < 4)
          continue;

        if (originalTerm.length() > 25)
          continue;

        List lis = new ArrayList(maxr);

        String[] suggestions = NGramSpeller.suggestUsingNGrams(spellingSearcher
            , originalTerm, ng1, ng2, maxr, bStart, bEnd,
            bTransposition, maxd, lis, true);

        if (suggestions.length > 0) {
          currentTerm.setSuggestedTerm(suggestions[0]);

          if (lis != null) {
            NGramSpeller.SpellSuggestionDetails detail = (NGramSpeller.SpellSuggestionDetails) lis
                .get(0);
            currentTerm.setSuggestedTermDocFreq(detail.docFreq);
          }

          // We use document frequencies of the original term and the suggested
          // term to guess
          // whether or not a term is mispelled. The criteria is as follows:
          //
          // 1. The term's document frequency must be under a constant threshold
          // 2. The suggested term's docFreq must be greater than the original
          // term's docFreq * constant factor
          //
          if ((currentTerm.originalDocFreq < docFreqThreshold)
              && ((currentTerm.originalDocFreq * factorThreshold) < (currentTerm.suggestedTermDocFreq))) {
            spellCheckerTerms.setHasMispelledTerms(true);
            currentTerm.setMispelled(true);
          }
        }

      }

    } catch (Throwable t) {
      t.printStackTrace();
    }

    return spellCheckerTerms;
  }

  /**
   * 
   * Parses the query and preserves characters and formatting surrounding terms
   * to be spell-checked. This is done so that we can present the query in the
   * "Did you mean: XYZ" message in the same format the user originally typed
   * it.
   * 
   * @param originalQuery
   *          text to be parsed
   * @return spell checker terms
   */
  public SpellCheckerTerms parseOriginalQuery(Query query, String queryString)
      throws IOException {
    String[] terms = query.getTerms();
    SpellCheckerTerms spellCheckerTerms = new SpellCheckerTerms();

    int previousTermPos = 0;
    for (int i = 0; i < terms.length; i++) {

      int termPos = queryString.toLowerCase().indexOf(terms[i]);

      String charsBefore = "";
      String charsAfter = "";

      // Is this the first term? If so, we need to check for characters
      // before the first term.
      if (i == 0) {

        if (termPos > 0) {
          charsBefore = queryString.substring(0, termPos);
        }

        // We're in-between terms...
      } else {
        int endOfLastTerm = previousTermPos + terms[i - 1].length();

        if (endOfLastTerm < termPos) {
          charsBefore = queryString.substring(endOfLastTerm, termPos);
        }
      }

      // Is this the last term? If so, we need to check for characters
      // after the last term.
      if (i == (terms.length - 1)) {

        int endOfCurrentTerm = termPos + terms[i].length();

        if (endOfCurrentTerm < queryString.length()) {
          charsAfter = queryString.substring(endOfCurrentTerm, queryString
              .length());
        }

      }

      previousTermPos = termPos;

      spellCheckerTerms.add(new SpellCheckerTerm(terms[i], charsBefore,
          charsAfter));

    }

    return spellCheckerTerms;

  }

  public SpellCheckerTerms parseOriginalQuery(String queryString)
      throws IOException {
    return parseOriginalQuery(Query.parse(queryString, conf), queryString);
  }

  /**
   * Retrieves docFreq as stored within spelling index. Alternatively, we could
   * simply consult the main index for a docFreq() of a term (which would be
   * faster) but it's nice to have a separate, spelling index that can stand on
   * its own.
   * 
   * @param term
   * @return document frequency of term
   */
  private int getDocFreq(String term) throws IOException {
    /*
     * Hits hits = this.spellingSearcher.getLuceneSearcher().search(new
     * TermQuery(new Term( NGramSpeller.F_WORD, term))); if (hits.length() > 0) {
     * Document doc = hits.doc(0); String docFreq =
     * doc.get(NGramSpeller.F_FREQ); return Integer.parseInt(docFreq); }
     */
    return 0;
  }

  public static void main(String[] args) throws Throwable {
    if (args.length < 1) {
      System.out.println("usage: SpellCheckerBean [ngrams spelling index]");
      return;
    }

    Configuration conf = NutchConfiguration.create();

    conf.set("spell.index.dir", args[0]);

    SpellCheckerBean checker = new SpellCheckerBean(conf);
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

    String line;

    while ((line = in.readLine()) != null) {
      Query query = Query.parse(line, conf);
      SpellCheckerTerms terms = checker.checkSpelling(query, line);
      StringBuffer buf = new StringBuffer();

      for (int i = 0; i < terms.size(); i++) {
        SpellCheckerTerm currentTerm = terms.getSpellCheckerTerm(i);
        buf.append(currentTerm.getCharsBefore());

        if (currentTerm.isMispelled()) {
          buf.append(currentTerm.getSuggestedTerm());
        } else {
          buf.append(currentTerm.getOriginalTerm());
        }
      }

      System.out.println("Spell checked: " + buf);
    }
  }

  public void init() {
    //do initialization here
  }

  public String[] suggest(Query query) {
    // TODO Auto-generated method stub
    return null;
  }

  public String getID() {
    return "SPELLER";
  }
}

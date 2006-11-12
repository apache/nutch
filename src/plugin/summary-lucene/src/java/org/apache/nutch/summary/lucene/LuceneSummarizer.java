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
package org.apache.nutch.summary.lucene;

// JDK imports
import java.io.StringReader;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;

// Lucene imports
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.WeightedTerm;

// Nutch imports
import org.apache.nutch.analysis.NutchDocumentAnalyzer;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.searcher.Summarizer;
import org.apache.nutch.searcher.Summary;
import org.apache.nutch.searcher.Summary.Ellipsis;
import org.apache.nutch.searcher.Summary.Fragment;
import org.apache.nutch.searcher.Summary.Highlight;


/** Implements hit summarization. */
public class LuceneSummarizer implements Summarizer {
  
  private final static String SEPARATOR = "###";
  private final static Formatter FORMATTER =
          new SimpleHTMLFormatter(SEPARATOR, SEPARATOR);

  /** Converts text to tokens. */
  private Analyzer analyzer = null;
  private Configuration conf = null;
  
  public LuceneSummarizer() { }
  
  private LuceneSummarizer(Configuration conf) {
    setConf(conf);
  }
  
  
  /* ----------------------------- *
   * <implementation:Configurable> *
   * ----------------------------- */
  
  public Configuration getConf() {
    return conf;
  }
  
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.analyzer = new NutchDocumentAnalyzer(conf);
  }
  
  /* ------------------------------ *
   * </implementation:Configurable> *
   * ------------------------------ */
  
  
  /* --------------------------- *
   * <implementation:Summarizer> *
   * --------------------------- */
  
  public Summary getSummary(String text, Query query) {

    String[] terms = query.getTerms();
    WeightedTerm[] weighted = new WeightedTerm[terms.length];
    for (int i=0; i<terms.length; i++) {
      weighted[i] = new WeightedTerm(1.0f, terms[i]);
    }
    Highlighter highlighter = new Highlighter(FORMATTER, new QueryScorer(weighted));
    TokenStream tokens = analyzer.tokenStream("content", new StringReader(text));
    Summary summary = new Summary();
    try {
      // TODO : The max number of fragments (3) should be configurable
      String[] result = highlighter.getBestFragments(tokens, text, 3);
      for (int i=0; i<result.length; i++) {
        String[] parts = result[i].split(SEPARATOR);
        boolean highlight = false;
        for (int j=0; j<parts.length; j++) {
          if (highlight) {
            summary.add(new Highlight(parts[j]));
          } else {
            summary.add(new Fragment(parts[j]));
          }
          highlight = !highlight;
        }
        summary.add(new Ellipsis());
      }
    } catch (Exception e) {
      // Nothing to do...
    }
    return summary;
  }

  /* ---------------------------- *
   * </implementation:Summarizer> *
   * ---------------------------- */
  
}

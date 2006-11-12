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
package org.apache.nutch.summary.basic;

// JDK imports
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;

// Lucene imports
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;

// Nutch imports
import org.apache.nutch.analysis.NutchDocumentAnalyzer;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.searcher.Summarizer;
import org.apache.nutch.searcher.Summary;
import org.apache.nutch.searcher.Summary.Ellipsis;
import org.apache.nutch.searcher.Summary.Fragment;
import org.apache.nutch.searcher.Summary.Highlight;
import org.apache.nutch.util.NutchConfiguration;


/** Implements hit summarization. */
public class BasicSummarizer implements Summarizer {
  
  private int sumContext = 5;
  private int sumLength = 20;
  private Analyzer analyzer = null;
  private Configuration conf = null;
  
  private final static Comparator ORDER_COMPARATOR = new Comparator() {
    public int compare(Object o1, Object o2) {
      return ((Excerpt) o1).getOrder() - ((Excerpt) o2).getOrder();
    }
  };
  
  private final static Comparator SCORE_COMPARATOR = new Comparator() {
    public int compare(Object o1, Object o2) {
      Excerpt excerpt1 = (Excerpt) o1;
      Excerpt excerpt2 = (Excerpt) o2;

      if (excerpt1 == null && excerpt2 != null) {
        return -1;
      } else if (excerpt1 != null && excerpt2 == null) {
        return 1;
      } else if (excerpt1 == null && excerpt2 == null) {
        return 0;
      }

      int numToks1 = excerpt1.numUniqueTokens();
      int numToks2 = excerpt2.numUniqueTokens();

      if (numToks1 < numToks2) {
        return -1;
      } else if (numToks1 == numToks2) {
        return excerpt1.numFragments() - excerpt2.numFragments();
      } else {
        return 1;
      }
    }
  };

  
  public BasicSummarizer() { }
  
  private BasicSummarizer(Configuration conf) {
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
    this.sumContext = conf.getInt("searcher.summary.context", 5);
    this.sumLength = conf.getInt("searcher.summary.length", 20);
  }
  
  /* ------------------------------ *
   * </implementation:Configurable> *
   * ------------------------------ */
  
  
  /* --------------------------- *
   * <implementation:Summarizer> *
   * --------------------------- */
  
  public Summary getSummary(String text, Query query) {
    
    // Simplistic implementation.  Finds the first fragments in the document
    // containing any query terms.
    //
    // TODO: check that phrases in the query are matched in the fragment
    
    Token[] tokens = getTokens(text);             // parse text to token array
    
    if (tokens.length == 0)
      return new Summary();
    
    String[] terms = query.getTerms();
    HashSet highlight = new HashSet();            // put query terms in table
    for (int i = 0; i < terms.length; i++)
      highlight.add(terms[i]);
    
    // A list to store document's excerpts.
    // (An excerpt is a Vector full of Fragments and Highlights)
    List excerpts = new ArrayList();
    
    //
    // Iterate through all terms in the document
    //
    int lastExcerptPos = 0;
    for (int i = 0; i < tokens.length; i++) {
      //
      // If we find a term that's in the query...
      //
      if (highlight.contains(tokens[i].termText())) {
        //
        // Start searching at a point SUM_CONTEXT terms back,
        // and move SUM_CONTEXT terms into the future.
        //
        int startToken = (i > sumContext) ? i - sumContext : 0;
        int endToken = Math.min(i + sumContext, tokens.length);
        int offset = tokens[startToken].startOffset();
        int j = startToken;
        
        //
        // Iterate from the start point to the finish, adding
        // terms all the way.  The end of the passage is always
        // SUM_CONTEXT beyond the last query-term.
        //
        Excerpt excerpt = new Excerpt(i);
        if (i != 0) {
          excerpt.add(new Summary.Ellipsis());
        }
        
        //
        // Iterate through as long as we're before the end of
        // the document and we haven't hit the max-number-of-items
        // -in-a-summary.
        //
        while ((j < endToken) && (j - startToken < sumLength)) {
          //
          // Now grab the hit-element, if present
          //
          Token t = tokens[j];
          if (highlight.contains(t.termText())) {
            excerpt.addToken(t.termText());
            excerpt.add(new Fragment(text.substring(offset, t.startOffset())));
            excerpt.add(new Highlight(text.substring(t.startOffset(),t.endOffset())));
            offset = t.endOffset();
            endToken = Math.min(j + sumContext, tokens.length);
          }
          
          j++;
        }
        
        lastExcerptPos = endToken;
        
        //
        // We found the series of search-term hits and added
        // them (with intervening text) to the excerpt.  Now
        // we need to add the trailing edge of text.
        //
        // So if (j < tokens.length) then there is still trailing
        // text to add.  (We haven't hit the end of the source doc.)
        // Add the words since the last hit-term insert.
        //
        if (j < tokens.length) {
          excerpt.add(new Fragment(text.substring(offset,tokens[j].endOffset())));
        }
        
        //
        // Remember how many terms are in this excerpt
        //
        excerpt.setNumTerms(j - startToken);
        
        //
        // Store the excerpt for later sorting
        //
        excerpts.add(excerpt);
        
        //
        // Start SUM_CONTEXT places away.  The next
        // search for relevant excerpts begins at i-SUM_CONTEXT
        //
        i = j + sumContext;
      }
    }
    
    // Sort the excerpts based on their score
    Collections.sort(excerpts, SCORE_COMPARATOR);
    
    //
    // If the target text doesn't appear, then we just
    // excerpt the first SUM_LENGTH words from the document.
    //
    if (excerpts.size() == 0) {
      Excerpt excerpt = new Excerpt(0);
      int excerptLen = Math.min(sumLength, tokens.length);
      lastExcerptPos = excerptLen;
      
      excerpt.add(new Fragment(text.substring(tokens[0].startOffset(), tokens[excerptLen-1].startOffset())));
      excerpt.setNumTerms(excerptLen);
      excerpts.add(excerpt);
    }
    
    //
    // Now choose the best items from the excerpt set.
    // Stop when we have enought excerpts to build our Summary.
    //
    double tokenCount = 0;
    int numExcerpt = excerpts.size()-1;
    List bestExcerpts = new ArrayList();
    while (tokenCount <= sumLength && numExcerpt >= 0) {
      Excerpt excerpt = (Excerpt) excerpts.get(numExcerpt--);
      bestExcerpts.add(excerpt);
      tokenCount += excerpt.getNumTerms();
    }    
    // Sort the best excerpts based on their natural order
    Collections.sort(bestExcerpts, ORDER_COMPARATOR);
    
    //
    // Now build our Summary from the best the excerpts.
    //
    tokenCount = 0;
    numExcerpt = 0;
    Summary s = new Summary();
    while (tokenCount <= sumLength && numExcerpt < bestExcerpts.size()) {
      Excerpt excerpt = (Excerpt) bestExcerpts.get(numExcerpt++);
      double tokenFraction = (1.0 * excerpt.getNumTerms()) / excerpt.numFragments();
      for (Enumeration e = excerpt.elements(); e.hasMoreElements(); ) {
        Fragment f = (Fragment) e.nextElement();
        // Don't add fragments if it takes us over the max-limit
        if (tokenCount + tokenFraction <= sumLength) {
          s.add(f);
        }
        tokenCount += tokenFraction;
      }
    }
    
    if (tokenCount > 0 && lastExcerptPos < tokens.length)
      s.add(new Ellipsis());
    return s;
  }
  
  /* ---------------------------- *
   * </implementation:Summarizer> *
   * ---------------------------- */
  
  /** Maximun number of tokens inspect in a summary . */
  private static final int token_deep = 2000;

  /**
   * Class Excerpt represents a single passage found in the document, with some
   * appropriate regions highlit.
   */
  class Excerpt {
    Vector passages = new Vector();
    SortedSet tokenSet = new TreeSet();
    int numTerms = 0;
    int order = 0;
    
    /**
     */
    public Excerpt(int order) {
      this.order = order;
    }
    
    /**
     */
    public void addToken(String token) {
      tokenSet.add(token);
    }
    
    /**
     * Return how many unique toks we have
     */
    public int numUniqueTokens() {
      return tokenSet.size();
    }
    
    /**
     * How many fragments we have.
     */
    public int numFragments() {
      return passages.size();
    }
    
    public void setNumTerms(int numTerms) {
      this.numTerms = numTerms;
    }
    
    public int getOrder() {
      return order;
    }
    
    public int getNumTerms() {
      return numTerms;
    }
    
    /**
     * Add a frag to the list.
     */
    public void add(Fragment fragment) {
      passages.add(fragment);
    }
    
    /**
     * Return an Enum for all the fragments
     */
    public Enumeration elements() {
      return passages.elements();
    }
  }
  
  
  private Token[] getTokens(String text) {
    ArrayList result = new ArrayList();
    TokenStream ts = analyzer.tokenStream("content", new StringReader(text));
    Token token = null;
    while (result.size()<token_deep) {
      try {
        token = ts.next();
      } catch (IOException e) {
        token = null;
      }
      if (token == null) { break; }
      result.add(token);
    }
    try {
      ts.close();
    } catch (IOException e) {
      // ignore
    }
    return (Token[]) result.toArray(new Token[result.size()]);
  }
  
  /**
   * Tests Summary-generation.  User inputs the name of a
   * text file and a query string
   */
  public static void main(String argv[]) throws IOException {
    // Test arglist
    if (argv.length < 2) {
      System.out.println("Usage: java org.apache.nutch.searcher.Summarizer <textfile> <queryStr>");
      return;
    }
    
    Configuration conf = NutchConfiguration.create();
    Summarizer s = new BasicSummarizer(conf);
    
    //
    // Parse the args
    //
    File textFile = new File(argv[0]);
    StringBuffer queryBuf = new StringBuffer();
    for (int i = 1; i < argv.length; i++) {
      queryBuf.append(argv[i]);
      queryBuf.append(" ");
    }
    
    //
    // Load the text file into a single string.
    //
    StringBuffer body = new StringBuffer();
    BufferedReader in = new BufferedReader(new FileReader(textFile));
    try {
      System.out.println("About to read " + textFile + " from " + in);
      String str = in.readLine();
      while (str != null) {
        body.append(str);
        str = in.readLine();
      }
    } finally {
      in.close();
    }
    
    // Convert the query string into a proper Query
    Query query = Query.parse(queryBuf.toString(), conf);
    System.out.println("Summary: '" + s.getSummary(body.toString(), query) + "'");
  }
}

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

package org.apache.nutch.analysis;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.nutch.searcher.Query.Phrase;
import org.apache.nutch.searcher.Query.Term;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.ObjectCache;

/** Construct n-grams for frequently occurring terms and phrases while indexing.
 * Optimize phrase queries to use the n-grams. Single terms are still indexed
 * too, with n-grams overlaid.  This is achieved through the use of {@link
 * Token#setPositionIncrement(int)}.*/
public class CommonGrams {
  private static final Log LOG = LogFactory.getLog(CommonGrams.class);
  private static final char SEPARATOR = '-';
  /** The key used to cache commonTerms in Configuration */
  private static final String KEY = CommonGrams.class.getName();

  private HashMap<String, HashSet<String>> commonTerms =
    new HashMap<String, HashSet<String>>();
  
  /**
   * The constructor.
   * @param conf
   */
  public CommonGrams(Configuration conf) {
      init(conf);
  }

  private static class Filter extends TokenFilter {
    private HashSet<String> common;
    private Token previous;
    private LinkedList<Token> gramQueue = new LinkedList<Token>();
    private LinkedList<Token> nextQueue = new LinkedList<Token>();
    private StringBuffer buffer = new StringBuffer();

    private final TermAttribute termAtt;
    private final PositionIncrementAttribute posIncrAtt;
    private final TypeAttribute typeAtt;
    private final OffsetAttribute offsetAtt;

    /** Construct an n-gram producing filter. */
    public Filter(TokenStream input, HashSet<String> common) {
      super(input);
      this.common = common;
      this.termAtt = getAttribute(TermAttribute.class);
      this.offsetAtt = getAttribute(OffsetAttribute.class);
      this.posIncrAtt = getAttribute(PositionIncrementAttribute.class);
      this.typeAtt = addAttribute(TypeAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      clearAttributes();
      Token t = next();
      if (t != null) {
        termAtt.setTermBuffer(t.termBuffer(), 0, t.termLength());
        offsetAtt.setOffset(t.startOffset(), t.endOffset());
        posIncrAtt.setPositionIncrement(t.getPositionIncrement());
        typeAtt.setType(t.type());
      }     
      return t != null;
    }

    private Token inputNext() throws IOException {
      if (super.input.incrementToken()) {
        Token t = new Token(
            termAtt.termBuffer(), 0, termAtt.termLength(),
            offsetAtt.startOffset(), offsetAtt.endOffset());
        t.setPositionIncrement(posIncrAtt.getPositionIncrement());
        t.setType(typeAtt.type());
        return t;
      }
      return null;
    }

    /** Inserts n-grams into a token stream. */
    public Token next() throws IOException {
      if (gramQueue.size() != 0)                  // consume any queued tokens
        return gramQueue.removeFirst();

      final Token token = popNext();
      if (token == null)
        return null;

      if (!isCommon(token)) {                     // optimize simple case
        previous = token;
        return token;
      }

      gramQueue.add(token);                       // queue the token

      ListIterator<Token> i = nextQueue.listIterator();
      Token gram = token;
      while (isCommon(gram)) {
        if (previous != null && !isCommon(previous)) // queue prev gram first
          gramQueue.addFirst(gramToken(previous, gram));

        Token next = peekNext(i);
        if (next == null)
          break;

        gram = gramToken(gram, next);             // queue next gram last
        gramQueue.addLast(gram);
      }

      previous = token;
      return gramQueue.removeFirst();
    }

    /** True iff token is for a common term. */
    private boolean isCommon(Token token) {
      return common != null && common.contains(token.term());
    }

    /** Pops nextQueue or, if empty, reads a new token. */
    private Token popNext() throws IOException {
      if (nextQueue.size() > 0)
        return nextQueue.removeFirst();
      else
        return inputNext();
    }

    /** Return next token in nextQueue, extending it when empty. */
    private Token peekNext(ListIterator<Token> i) throws IOException {
      if (!i.hasNext()) {
        Token next = inputNext();
        if (next == null)
          return null;
        i.add(next);
        i.previous();
      }
      return i.next();
    }

    /** Construct a compound token. */
    private Token gramToken(Token first, Token second) {
      buffer.setLength(0);
      buffer.append(first.term());
      buffer.append(SEPARATOR);
      buffer.append(second.term());
      Token result = new Token(buffer.toString(),
                               first.startOffset(), second.endOffset(),
                               "gram");
      result.setPositionIncrement(0);
      return result;
    }
  }

  /** Construct using the provided config file. */
  private void init(Configuration conf) {
    ObjectCache objectCache = ObjectCache.get(conf);
    // First, try to retrieve some commonTerms cached in configuration.
    commonTerms = (HashMap<String, HashSet<String>>) objectCache.getObject(KEY);
    if (commonTerms != null) { return; }

    // Otherwise, read the terms.file
    try {
      commonTerms = new HashMap<String, HashSet<String>>();
      Reader reader = conf.getConfResourceAsReader
        (conf.get("analysis.common.terms.file"));
      BufferedReader in = new BufferedReader(reader);
      String line;
      while ((line = in.readLine()) != null) {
        line = line.trim();
        if (line.startsWith("#") || "".equals(line)) // skip comments
          continue;
        TokenStream ts = new NutchDocumentTokenizer(new StringReader(line));
        TermAttribute ta = ts.getAttribute(TermAttribute.class);
        if (!ts.incrementToken()) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Line does not contain a field name: " + line);
          }
          continue;
        }
        String field = ta.term();
        if (!ts.incrementToken()) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Line contains only a field name, no word: " + line);
          }
          continue;
        }
        String gram = ta.term();
        while (ts.incrementToken()) {
          gram = gram + SEPARATOR + ta.term();
        }
        HashSet<String> table = commonTerms.get(field);
        if (table == null) {
          table = new HashSet<String>();
          commonTerms.put(field, table);
        }
        table.add(gram);
      }
      objectCache.setObject(KEY, commonTerms);
    } catch (IOException e) {
      throw new RuntimeException(e.toString());
    }
  }

  /** Construct a token filter that inserts n-grams for common terms.  For use
   * while indexing documents.  */
  public TokenFilter getFilter(TokenStream ts, String field) {
    return new Filter(ts, commonTerms.get(field));
  }

  /** Utility to convert an array of Query.Terms into a token stream. */
  private static class ArrayTokens extends TokenStream {
    private Term[] terms;
    private int index;
    private final TermAttribute termAttr;
    private final PositionIncrementAttribute posAttr;
    private final OffsetAttribute offsetAttr;

    public ArrayTokens(Phrase phrase) {
      this.terms = phrase.getTerms();
      this.termAttr = addAttribute(TermAttribute.class);
      this.posAttr = addAttribute(PositionIncrementAttribute.class);
      this.offsetAttr = addAttribute(OffsetAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (index == terms.length)
        return false;

      clearAttributes();
      termAttr.setTermBuffer(terms[index].toString());
      posAttr.setPositionIncrement(1);
      offsetAttr.setOffset(index, ++index);
      return true;
    }
  }

  /** Optimizes phrase queries to use n-grams when possible. */
  public String[] optimizePhrase(Phrase phrase, String field) {
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("Optimizing " + phrase + " for " + field);
    }
    ArrayList<String> result = new ArrayList<String>();
    TokenStream ts = getFilter(new ArrayTokens(phrase), field);
    String prev = null;
    TermAttribute ta = ts.getAttribute(TermAttribute.class);
    PositionIncrementAttribute pa = ts.getAttribute(PositionIncrementAttribute.class);
    int position = 0;
    try {
      while (ts.incrementToken()) {
        if (pa.getPositionIncrement() != 0 && prev != null)
          result.add(prev);
        prev = ta.term();
        position += pa.getPositionIncrement();
        if ((position + arity(ta.term())) == phrase.getTerms().length)
          break;
      }
    } catch (IOException e) {
      throw new RuntimeException(e.toString());
    }
    if (prev != null)
      result.add(prev);

    return result.toArray(new String[result.size()]);
  }

  private int arity(String gram) {
    int index = 0;
    int arity = 0;
    while ((index = gram.indexOf(SEPARATOR, index+1)) != -1) {
      arity++;
    }
    return arity;
  }

  /** For debugging. */
  public static void main(String[] args) throws Exception {
    StringBuffer text = new StringBuffer();
    for (int i = 0; i < args.length; i++) {
      text.append(args[i]);
      text.append(' ');
    }
    TokenStream ts = new NutchDocumentTokenizer(new StringReader(text.toString()));
    CommonGrams commonGrams = new CommonGrams(NutchConfiguration.create());
    ts = commonGrams.getFilter(ts, "url");
    TermAttribute ta = ts.getAttribute(TermAttribute.class);
    OffsetAttribute oa = ts.getAttribute(OffsetAttribute.class);
    PositionIncrementAttribute pia = ts.getAttribute(PositionIncrementAttribute.class);
    while (ts.incrementToken()) {
      System.out.println("Token: " + ta.term() + " offs:" + oa.startOffset() + "-" + oa.endOffset()
          + " incr: " + pia.getPositionIncrement());
    }
    String[] optimized = commonGrams.optimizePhrase(new Phrase(args), "url");
    System.out.print("Optimized: ");
    for (int i = 0; i < optimized.length; i++) {
      System.out.print(optimized[i] + " ");
    }
    System.out.println();
  }
  
}

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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;

import java.io.*;
import java.util.*;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.*;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.ObjectCache;
import org.apache.nutch.searcher.Query.*;

/** Construct n-grams for frequently occuring terms and phrases while indexing.
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

    /** Construct an n-gram producing filter. */
    public Filter(TokenStream input, HashSet<String> common) {
      super(input);
      this.common = common;
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
      return common != null && common.contains(token.termText());
    }

    /** Pops nextQueue or, if empty, reads a new token. */
    private Token popNext() throws IOException {
      if (nextQueue.size() > 0)
        return nextQueue.removeFirst();
      else
        return input.next();
    }

    /** Return next token in nextQueue, extending it when empty. */
    private Token peekNext(ListIterator<Token> i) throws IOException {
      if (!i.hasNext()) {
        Token next = input.next();
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
      buffer.append(first.termText());
      buffer.append(SEPARATOR);
      buffer.append(second.termText());
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
        Token token = ts.next();
        if (token == null) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Line does not contain a field name: " + line);
          }
          continue;
        }
        String field = token.termText();
        token = ts.next();
        if (token == null) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Line contains only a field name, no word: " + line);
          }
          continue;
        }
        String gram = token.termText();
        while ((token = ts.next()) != null) {
          gram = gram + SEPARATOR + token.termText();
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

    public ArrayTokens(Phrase phrase) {
      this.terms = phrase.getTerms();
    }

    public Token next() {
      if (index == terms.length)
        return null;
      else
        return new Token(terms[index].toString(), index, ++index);
    }
  }

  /** Optimizes phrase queries to use n-grams when possible. */
  public String[] optimizePhrase(Phrase phrase, String field) {
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("Optimizing " + phrase + " for " + field);
    }
    ArrayList<String> result = new ArrayList<String>();
    TokenStream ts = getFilter(new ArrayTokens(phrase), field);
    Token token, prev=null;
    int position = 0;
    try {
      while ((token = ts.next()) != null) {
        if (token.getPositionIncrement() != 0 && prev != null)
          result.add(prev.termText());
        prev = token;
        position += token.getPositionIncrement();
        if ((position + arity(token.termText())) == phrase.getTerms().length)
          break;
      }
    } catch (IOException e) {
      throw new RuntimeException(e.toString());
    }
    if (prev != null)
      result.add(prev.termText());

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
    Token token;
    while ((token = ts.next()) != null) {
      System.out.println("Token: " + token);
    }
    String[] optimized = commonGrams.optimizePhrase(new Phrase(args), "url");
    System.out.print("Optimized: ");
    for (int i = 0; i < optimized.length; i++) {
      System.out.print(optimized[i] + " ");
    }
    System.out.println();
  }
  
}

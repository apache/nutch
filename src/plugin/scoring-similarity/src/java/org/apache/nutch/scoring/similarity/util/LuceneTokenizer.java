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
package org.apache.nutch.scoring.similarity.util;

import java.io.StringReader;
import java.util.List;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.nutch.scoring.similarity.util.LuceneAnalyzerUtil.StemFilterType;

public class LuceneTokenizer {

  private TokenStream tokenStream; 
  private TokenizerType tokenizer;
  private StemFilterType stemFilterType;
  private CharArraySet stopSet = null;

  public static enum TokenizerType {CLASSIC, STANDARD}

  /**
   * Creates a tokenizer based on param values
   * @param content - The text to tokenize
   * @param tokenizer - the type of tokenizer to use CLASSIC or DEFAULT 
   * @param useStopFilter - if set to true the token stream will be filtered using default Lucene stopset 
   * @param stemFilterType - Type of stemming to perform 
   */
  public LuceneTokenizer(String content, TokenizerType tokenizer, boolean useStopFilter, StemFilterType stemFilterType) {
    this.tokenizer = tokenizer;
    this.stemFilterType = stemFilterType;
    if(useStopFilter) {
      stopSet = StandardAnalyzer.STOP_WORDS_SET;
    }
    tokenStream = createTokenStream(content);
  }

  /**
   * Creates a tokenizer based on param values
   * @param content - The text to tokenize
   * @param tokenizer - the type of tokenizer to use CLASSIC or DEFAULT 
   * @param stopWords - Provide a set of user defined stop words
   * @param addToDefault - If set to true, the stopSet words will be added to the Lucene default stop set.
   * If false, then only the user provided words will be used as the stop set
   * @param stemFilterType
   */
  public LuceneTokenizer(String content, TokenizerType tokenizer, List<String> stopWords, boolean addToDefault, StemFilterType stemFilterType) {
    this.tokenizer = tokenizer;
    this.stemFilterType = stemFilterType;
    if(addToDefault) {
      CharArraySet stopSet = CharArraySet.copy(StandardAnalyzer.STOP_WORDS_SET);;
      for(String word : stopWords){
        stopSet.add(word);
      }
      this.stopSet = stopSet;
    }
    else {
      stopSet = new CharArraySet(stopWords, true);
    }
    tokenStream = createTokenStream(content);
  }

  /**
   * Returns the tokenStream created by the Tokenizer
   * @return
   */
  public TokenStream getTokenStream() {
    return tokenStream;
  }
  
  /**
   * Creates a tokenizer for the ngram model based on param values
   * @param content - The text to tokenize
   * @param tokenizer - the type of tokenizer to use CLASSIC or DEFAULT 
   * @param stemFilterType - Type of stemming to perform
   * @param mingram - Value of mingram for tokenizing
   * @param maxgram - Value of maxgram for tokenizing
   */
  public LuceneTokenizer(String content, TokenizerType tokenizer, StemFilterType stemFilterType, int mingram, int maxgram) {
    this.tokenizer = tokenizer;
    this.stemFilterType = stemFilterType;
    tokenStream = createNGramTokenStream(content, mingram, maxgram);
  }
  
  private TokenStream createTokenStream(String content) {
    tokenStream = generateTokenStreamFromText(content, tokenizer);
    tokenStream = new LowerCaseFilter(tokenStream);
    if(stopSet != null) {
      tokenStream = applyStopFilter(stopSet);
    }
    tokenStream = applyStemmer(stemFilterType);
    return tokenStream;
  }

  private TokenStream generateTokenStreamFromText(String content, TokenizerType tokenizerType){
    Tokenizer tokenizer = null;
    switch(tokenizerType){
    case CLASSIC:
      tokenizer = new ClassicTokenizer();
      break;

    case STANDARD:
    default:
      tokenizer = new StandardTokenizer();
    }

    tokenizer.setReader(new StringReader(content));

    tokenStream = tokenizer;

    return tokenStream;
  }

  private TokenStream createNGramTokenStream(String content, int mingram, int maxgram) {
    Tokenizer tokenizer = new StandardTokenizer();
    tokenizer.setReader(new StringReader(content));
    tokenStream = new LowerCaseFilter(tokenizer);
    tokenStream = applyStemmer(stemFilterType);
    ShingleFilter shingleFilter = new ShingleFilter(tokenStream, mingram, maxgram);
    shingleFilter.setOutputUnigrams(false);
    tokenStream = (TokenStream)shingleFilter;
    return tokenStream;
  }

  private TokenStream applyStopFilter(CharArraySet stopWords) {
    tokenStream = new StopFilter(tokenStream, stopWords); 
    return tokenStream;
  }

  private TokenStream applyStemmer(StemFilterType stemFilterType) {
    switch(stemFilterType){
    case ENGLISHMINIMALSTEM_FILTER:
      tokenStream = new EnglishMinimalStemFilter(tokenStream);
      break;
    case PORTERSTEM_FILTER:
      tokenStream = new PorterStemFilter(tokenStream);
      break;
    default:
      break;
    }

    return tokenStream; 
  }
}

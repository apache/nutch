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

import java.io.Reader;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.CharArraySet;

/**
 * Creates a custom analyzer based on user provided inputs
 *
 */
public class LuceneAnalyzerUtil extends Analyzer{ 
  
  public static enum StemFilterType { PORTERSTEM_FILTER, ENGLISHMINIMALSTEM_FILTER, NONE }
  
  private static StemFilterType stemFilterType;
  private static CharArraySet stopSet;
  
  
  /**
   * Creates an analyzer instance based on Lucene default stopword set if @param useStopFilter is set to true
   */
  public LuceneAnalyzerUtil(StemFilterType stemFilterType, boolean useStopFilter) {
    LuceneAnalyzerUtil.stemFilterType = stemFilterType;
    if(useStopFilter) {
      stopSet = StandardAnalyzer.STOP_WORDS_SET;
    }
    else {
      stopSet = null;
    }
  }
  
  /**
   * Creates an analyzer instance based on user provided stop words. If @param addToDefault is set to true, then 
   * user provided stop words will be added to the Lucene default stopset.
   */
  public LuceneAnalyzerUtil(StemFilterType stemFilterType, List<String> stopWords, boolean addToDefault) {
    LuceneAnalyzerUtil.stemFilterType = stemFilterType;
    if(addToDefault) {
      stopSet.addAll(stopWords);
    }
    else {
      stopSet = StopFilter.makeStopSet(stopWords);
    }
  }
    
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    Tokenizer source = new ClassicTokenizer();
    TokenStream filter = new LowerCaseFilter(source);
    if(stopSet != null) {
      filter = new StopFilter(filter, stopSet);
    }
    
    switch(stemFilterType){
    case PORTERSTEM_FILTER:
      filter = new PorterStemFilter(filter);
      break;
    case ENGLISHMINIMALSTEM_FILTER:
      filter = new EnglishMinimalStemFilter(filter);
      break;
    default:
      break;        
    }
    return new TokenStreamComponents(source, filter);
  }

}

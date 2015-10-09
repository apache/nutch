package org.apache.nutch.scoring.similarity.util;

import java.io.StringReader;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
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
   * @param stopSet - Provide a set of user defined stop words
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
  
  private TokenStream createTokenStream(String content) {
    tokenStream = generateTokenStreamFromText(content, tokenizer);
    tokenStream = new LowerCaseFilter(tokenStream);
    if(stopSet != null) {
      tokenStream = applyStopFilter(stopSet);
    }
    tokenStream = applyStemmer(stemFilterType);
    return tokenStream;
  }
  
  private TokenStream generateTokenStreamFromText(String content, TokenizerType tokenizer){
    switch(tokenizer){
    case CLASSIC:
      tokenStream = new ClassicTokenizer(new StringReader(content));
      break;
      
    case STANDARD:
      tokenStream = new StandardTokenizer(new StringReader(content));
    }
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

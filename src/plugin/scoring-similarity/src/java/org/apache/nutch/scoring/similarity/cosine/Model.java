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
package org.apache.nutch.scoring.similarity.cosine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.nutch.scoring.similarity.util.LuceneAnalyzerUtil.StemFilterType;
import org.apache.nutch.scoring.similarity.util.LuceneTokenizer;
import org.apache.nutch.scoring.similarity.util.LuceneTokenizer.TokenizerType;
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class creates a model used to store Document vector representation of the corpus. 
 *
 */
public class Model {

  //Currently only one file, but in future could accept a corpus hence an ArrayList
  public static ArrayList<DocVector> docVectors = new ArrayList<>(); 
  private static final Logger LOG = LoggerFactory.getLogger(Model.class);
  public static boolean isModelCreated = false;
  private static List<String> stopWords;

  public static synchronized void createModel(Configuration conf) throws IOException {
    if(isModelCreated) {
      LOG.info("Model exists, skipping model creation");
      return;
    }
    LOG.info("Creating Cosine model");
    try {
      //If user has specified a stopword file other than the template
      if(!conf.get("scoring.similarity.stopword.file").equals("stopwords.txt.template")) {
        stopWords = new ArrayList<String>();
        String stopWord;
        BufferedReader br = new BufferedReader(conf.getConfResourceAsReader((conf.get("scoring.similarity.stopword.file"))));
        while ((stopWord = br.readLine()) != null) {
          stopWords.add(stopWord);
        }
        LOG.info("Loaded custom stopwords from {}",conf.get("scoring.similarity.stopword.file"));
      }

      //Check if user has specified n for ngram cosine model
      int ngram = conf.getInt("scoring.similarity.ngrams", 1);
      LOG.info("Value of ngram: {}",ngram);

      // TODO : Allow for corpus of documents to be provided as gold standard. 
      String line;
      StringBuilder sb = new StringBuilder();
      BufferedReader br = new BufferedReader(conf.getConfResourceAsReader((conf.get("cosine.goldstandard.file"))));
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
      DocVector goldStandard = createDocVector(sb.toString(), ngram);
      if(goldStandard!=null)
        docVectors.add(goldStandard);
      else {
        throw new Exception("Could not create DocVector for goldstandard");
      }
    } catch (Exception e) {
      LOG.warn("Failed to add {} to model : {}",conf.get("cosine.goldstandard.file","goldstandard.txt.template"), 
          StringUtils.stringifyException(e));
    }
    if(docVectors.size()>0) {
      LOG.info("Cosine model creation complete");
      isModelCreated = true;
    }
    else
      LOG.info("Cosine model creation failed");
  }

  /**
   * Used to create a DocVector from given String text. Used during the parse stage of the crawl 
   * cycle to create a DocVector of the currently parsed page from the parseText attribute value
   * @param content
   * @param ngram
   */
  public static DocVector createDocVector(String content, int ngram) {
    LuceneTokenizer tokenizer;
    
    if(ngram > 1){
      LOG.info("Using Ngram Cosine Model, user specified ngram value : {}", ngram);
      tokenizer = new LuceneTokenizer(content, TokenizerType.STANDARD, StemFilterType.PORTERSTEM_FILTER, ngram);
    }
    else if(stopWords!=null) {
      tokenizer = new LuceneTokenizer(content, TokenizerType.STANDARD, stopWords, true, 
          StemFilterType.PORTERSTEM_FILTER);
    }
    else {
      tokenizer = new LuceneTokenizer(content, TokenizerType.STANDARD, true, 
          StemFilterType.PORTERSTEM_FILTER);
    }
    TokenStream tStream = tokenizer.getTokenStream();
    HashMap<String, Integer> termVector = new HashMap<>();
    try {
      CharTermAttribute charTermAttribute = tStream.addAttribute(CharTermAttribute.class);
      tStream.reset();
      while(tStream.incrementToken()) {
        String term = charTermAttribute.toString();
        LOG.debug(term);
        if(termVector.containsKey(term)) {
          int count = termVector.get(term);
          count++;
          termVector.put(term, count);
        }
        else {
          termVector.put(term, 1);
        }
      }
      DocVector docVector = new DocVector();
      docVector.setTermFreqVector(termVector);
      return docVector;
    } catch (IOException e) {
      LOG.error("Error creating DocVector : {}",StringUtils.stringifyException(e));
    }
    return null;
  }

  public static float computeCosineSimilarity(DocVector docVector) {
    float scores[] = new float[docVectors.size()];
    int i=0;
    float maxScore = 0;
    for(DocVector corpusDoc : docVectors) {
      float numerator = docVector.dotProduct(corpusDoc);
      float denominator = docVector.getL2Norm()*corpusDoc.getL2Norm();
      float currentScore = numerator/denominator;
      scores[i++] = currentScore;
      maxScore = (currentScore>maxScore)? currentScore : maxScore;
    }
    // Returning the max score amongst all documents in the corpus
    return maxScore;
  }
}
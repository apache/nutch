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

package org.apache.nutch.urlfilter.model;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;
import org.apache.mahout.vectorizer.TFIDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;

public class NaiveBayesClassifier {

  private static final Logger LOG = LoggerFactory
      .getLogger(NaiveBayesClassifier.class);

  public static Map<String, Integer> readDictionnary(Configuration conf,
      Path dictionnaryPath) {
    Map<String, Integer> dictionnary = new HashMap<String, Integer>();
    for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(
        dictionnaryPath, true, conf)) {
      dictionnary.put(pair.getFirst().toString(), pair.getSecond().get());
    }
    return dictionnary;
  }

  public static Map<Integer, Long> readDocumentFrequency(Configuration conf,
      Path documentFrequencyPath) {
    Map<Integer, Long> documentFrequency = new HashMap<Integer, Long>();
    for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(
        documentFrequencyPath, true, conf)) {
      documentFrequency.put(pair.getFirst().get(), pair.getSecond().get());
    }
    return documentFrequency;
  }

  public static void createModel(String inputTrainFilePath) throws Exception {

    String[] args1 = new String[4];

    args1[0] = "-i";
    args1[1] = "outseq";
    args1[2] = "-o";
    args1[3] = "vectors";

    String[] args2 = new String[9];

    args2[0] = "-i";
    args2[1] = "vectors/tfidf-vectors";
    args2[2] = "-el";
    args2[3] = "-li";
    args2[4] = "labelindex";
    args2[5] = "-o";
    args2[6] = "model";
    args2[7] = "-ow";
    args2[8] = "-c";

    convertToSeq(inputTrainFilePath, "outseq");

    SparseVectorsFromSequenceFiles.main(args1);

    TrainNaiveBayesJob.main(args2);
  }

  public static String classify(String text) throws IOException {
    return classify(text, "model", "labelindex", "vectors/dictionary.file-0",
        "vectors/df-count/part-r-00000");
  }

  public static String classify(String text, String modelPath,
      String labelIndexPath, String dictionaryPath, String documentFrequencyPath)
      throws IOException {

    Configuration configuration = new Configuration();

    // model is a matrix (wordId, labelId) => probability score
    NaiveBayesModel model = NaiveBayesModel.materialize(new Path(modelPath),
        configuration);

    StandardNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(
        model);

    // labels is a map label => classId
    Map<Integer, String> labels = BayesUtils.readLabelIndex(configuration,
        new Path(labelIndexPath));
    Map<String, Integer> dictionary = readDictionnary(configuration, new Path(
        dictionaryPath));
    Map<Integer, Long> documentFrequency = readDocumentFrequency(configuration,
        new Path(documentFrequencyPath));

    // analyzer used to extract word from text
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_43);
    // int labelCount = labels.size();
    int documentCount = documentFrequency.get(-1).intValue();

    Multiset<String> words = ConcurrentHashMultiset.create();

    // extract words from text
    TokenStream ts = analyzer.tokenStream("text", new StringReader(text));
    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
    ts.reset();
    int wordCount = 0;
    while (ts.incrementToken()) {
      if (termAtt.length() > 0) {
        String word = ts.getAttribute(CharTermAttribute.class).toString();
        Integer wordId = dictionary.get(word);
        // if the word is not in the dictionary, skip it
        if (wordId != null) {
          words.add(word);
          wordCount++;
        }
      }
    }

    ts.end();
    ts.close();
    // create vector wordId => weight using tfidf
    Vector vector = new RandomAccessSparseVector(10000);
    TFIDF tfidf = new TFIDF();
    for (Multiset.Entry<String> entry : words.entrySet()) {
      String word = entry.getElement();
      int count = entry.getCount();
      Integer wordId = dictionary.get(word);
      Long freq = documentFrequency.get(wordId);
      double tfIdfValue = tfidf.calculate(count, freq.intValue(), wordCount,
          documentCount);
      vector.setQuick(wordId, tfIdfValue);
    }
    // one score for each label

    Vector resultVector = classifier.classifyFull(vector);
    double bestScore = -Double.MAX_VALUE;
    int bestCategoryId = -1;
    for (Element element : resultVector.all()) {
      int categoryId = element.index();
      double score = element.get();
      if (score > bestScore) {
        bestScore = score;
        bestCategoryId = categoryId;
      }

    }

    analyzer.close();
    return labels.get(bestCategoryId);

  }

  static void convertToSeq(String inputFileName, String outputDirName)
      throws IOException {
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.get(configuration);
    Writer writer = new SequenceFile.Writer(fs, configuration, new Path(
        outputDirName + "/chunk-0"), Text.class, Text.class);
    BufferedReader reader = null;
    reader = new BufferedReader(
        configuration.getConfResourceAsReader(inputFileName));
    Text key = new Text();
    Text value = new Text();
    long uniqueid=0;
    while (true) {
      uniqueid++;
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      String[] tokens = line.split("\t", 2);
      if (tokens.length != 2) {
        continue;
      }
      String category = tokens[0];
      String id = ""+uniqueid;
      String message = tokens[1];
      key.set("/" + category + "/" + id);
      value.set(message);
      writer.append(key, value);

    }
    reader.close();
    writer.close();

  }

  
}

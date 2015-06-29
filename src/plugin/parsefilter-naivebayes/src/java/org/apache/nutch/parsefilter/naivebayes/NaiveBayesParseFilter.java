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
package org.apache.nutch.parsefilter.naivebayes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

/**
 * Html Parse filter that classifies the outlinks from the parseresult as
 * relevant or irrelevant based on the parseText's relevancy (using a training
 * file where you can give positive and negative example texts see the
 * description of parsefilter.naivebayes.trainfile) and if found irrelevent it
 * gives the link a second chance if it contains any of the words from the list
 * given in parsefilter.naivebayes.wordlist. CAUTION: Set the parser.timeout to
 * -1 or a bigger value than 30, when using this classifier.
 */
public class NaiveBayesParseFilter implements HtmlParseFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(NaiveBayesParseFilter.class);

  public static final String TRAINFILE_MODELFILTER = "parsefilter.naivebayes.trainfile";
  public static final String DICTFILE_MODELFILTER = "parsefilter.naivebayes.wordlist";

  private Configuration conf;
  private String inputFilePath;
  private String dictionaryFile;
  private ArrayList<String> wordlist = new ArrayList<String>();

  public boolean filterParse(String text) {

    try {
      return classify(text);
    } catch (IOException e) {
      LOG.error("Error occured while classifying:: " + text + " ::"
          + StringUtils.stringifyException(e));
    }

    return false;
  }

  public boolean filterUrl(String url) {

    return containsWord(url, wordlist);

  }

  public boolean classify(String text) throws IOException {

    // if classified as relevant "1" then return true
    if (NaiveBayesClassifier.classify(text).equals("1"))
      return true;
    return false;
  }

  public void train() throws Exception {
    // check if the model file exists, if it does then don't train
    if (!FileSystem.get(conf).exists(new Path("model"))) {
      LOG.info("Training the Naive Bayes Model");
      NaiveBayesClassifier.createModel(inputFilePath);
    } else {
      LOG.info("Model file already exists. Skipping training.");
    }
  }

  public boolean containsWord(String url, ArrayList<String> wordlist) {
    for (String word : wordlist) {
      if (url.contains(word)) {
        return true;
      }
    }

    return false;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    inputFilePath = conf.get(TRAINFILE_MODELFILTER);
    dictionaryFile = conf.get(DICTFILE_MODELFILTER);
    String message = null;
    boolean errorloading = false;

    try {
      if (inputFilePath == null || inputFilePath.trim().length() == 0) {
        message = "ParseFilter: NaiveBayes: trainfile not set in parsefilter.naivebayes.trainfile";
        errorloading = true;
      }

      if (dictionaryFile == null || dictionaryFile.trim().length() == 0) {

        message = "ParseFilter: NaiveBayes: wordlist not set in parsefilter.naivebayes.wordlist";
        errorloading = true;
      }

      if (!FileSystem.get(conf).exists(new Path("conf/" + inputFilePath))) {
        message = "ParseFilter: NaiveBayes: train file: " + inputFilePath
            + " does not exist!";
        errorloading = true;
      }
      if (!FileSystem.get(conf).exists(new Path("conf/" + dictionaryFile))) {
        message = "ParseFilter: NaiveBayes: wordlist file: " + dictionaryFile
            + " does not exist!";
        errorloading = true;
      }

      if (errorloading) {
        if (LOG.isErrorEnabled()) {
          LOG.error(message);
        }
        throw new IOException(message);
      }

      BufferedReader br = null;

      String CurrentLine;
      Reader reader = conf.getConfResourceAsReader(dictionaryFile);
      br = new BufferedReader(reader);
      while ((CurrentLine = br.readLine()) != null) {
        wordlist.add(CurrentLine);
      }

    } catch (IOException e) {
      LOG.error(e.getMessage()
          + " Error occured during loading. training will be skipped");

    }

    try {
      if (!errorloading)
        train();
    } catch (Exception e) {

      LOG.error("Error occured while training:: "
          + StringUtils.stringifyException(e));

    }

  }

  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public ParseResult filter(Content content, ParseResult parseResult,
      HTMLMetaTags metaTags, DocumentFragment doc) {

    Parse parse = parseResult.get(content.getUrl());

    String url = content.getBaseUrl();
    ArrayList<Outlink> tempOutlinks = new ArrayList<Outlink>();
    String text = parse.getText();

    try {
      if (FileSystem.get(conf).exists(new Path("model")) && !filterParse(text)) { // kick
                                                                                  // in
                                                                                  // the
                                                                                  // second
                                                                                  // tier
        // if parent page found
        // irrelevent
        LOG.info("ParseFilter: NaiveBayes: Page found irrelevent:: " + url);
        LOG.info("Checking outlinks");

        Outlink[] out = null;
        for (int i = 0; i < parse.getData().getOutlinks().length; i++) {
          LOG.info("ParseFilter: NaiveBayes: Outlink to check:: "
              + parse.getData().getOutlinks()[i].getToUrl());
          if (filterUrl(parse.getData().getOutlinks()[i].getToUrl())) {
            tempOutlinks.add(parse.getData().getOutlinks()[i]);
            LOG.info("ParseFilter: NaiveBayes: found relevent");

          } else {
            LOG.info("ParseFilter: NaiveBayes: found irrelevent");
          }
        }
        out = new Outlink[tempOutlinks.size()];
        for (int i = 0; i < tempOutlinks.size(); i++) {
          out[i] = tempOutlinks.get(i);
        }
        parse.getData().setOutlinks(out);

      } else {
        LOG.info("ParseFilter: NaiveBayes: Page found relevent:: " + url);
      }
    } catch (IOException e) {
      LOG.error("ParseFilter: NaiveBayes: Error reading the model file. Skippking classification.");
    }

    return parseResult;
  }

}

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
package org.apache.nutch.htmlparsefilter.naivebayes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;

import java.io.Reader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Html Parse filter that classifies the outlinks from the parseresult as
 * relevant or irrelevant based on the parseText's relevancy (using a training
 * file where you can give positive and negative example texts see the
 * description of htmlparsefilter.naivebayes.trainfile) and if found irrelevent
 * it gives the link a second chance if it contains any of the words from the
 * list given in htmlparsefilter.naivebayes.wordlist. CAUTION: Set the
 * parser.timeout to -1 or a bigger value than 30, when using this classifier.
 */
public class NaiveBayesHTMLParseFilter implements HtmlParseFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(NaiveBayesHTMLParseFilter.class);

  public static final String TRAINFILE_MODELFILTER = "htmlparsefilter.naivebayes.trainfile";
  public static final String DICTFILE_MODELFILTER = "htmlparsefilter.naivebayes.wordlist";

  private Configuration conf;
  private String inputFilePath;
  private String dictionaryFile;
  private ArrayList<String> wordlist = new ArrayList<String>();

  public NaiveBayesHTMLParseFilter() {

  }

  public boolean filterParse(String text) {

    try {
      return classify(text);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      LOG.error("Error occured while classifying:: " + text);

    }

    return false;
  }

  public boolean filterUrl(String url) {

    return containsWord(url, wordlist);

  }

  public boolean classify(String text) throws IOException {

    // if classified as relevent "1" then return true
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
      LOG.info("Model already exists. Skipping training.");
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
    if (inputFilePath == null || inputFilePath.trim().length() == 0
        || dictionaryFile == null || dictionaryFile.trim().length() == 0) {
      String message = "Model URLFilter: trainfile or wordlist not set in the urlfilter.model.trainfile or urlfilter.model.wordlist";
      if (LOG.isErrorEnabled()) {
        LOG.error(message);
      }
      throw new IllegalArgumentException(message);
    }
    BufferedReader br = null;

    try {

      String CurrentLine;

      Reader reader = conf.getConfResourceAsReader(dictionaryFile);
      br = new BufferedReader(reader);
      while ((CurrentLine = br.readLine()) != null) {
        wordlist.add(CurrentLine);
      }

    } catch (IOException e) {
      LOG.error("Error occured while reading the wordlist");

    } finally {
      try {
        if (br != null)
          br.close();
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }

    try {

      train();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      LOG.error("Error occured while training");
      e.getStackTrace();

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
    String title = parse.getData().getTitle();
    ParseStatus status = parse.getData().getStatus();
    String text = parse.getText();

    if (!filterParse(text)) { // kick in the second tier
      // if parent page found
      // irrelevent
      LOG.info("ModelURLFilter: Page found irrelevent:: " + url);
      LOG.info("Checking outlinks");

      Outlink[] out = null;
      for (int i = 0; i < parse.getData().getOutlinks().length; i++) {
        LOG.info("ModelURLFilter: Outlink to check:: "
            + parse.getData().getOutlinks()[i].getToUrl());
        if (filterUrl(parse.getData().getOutlinks()[i].getToUrl())) {
          tempOutlinks.add(parse.getData().getOutlinks()[i]);
          LOG.info("ModelURLFilter: found relevent");

        } else {
          LOG.info("ModelURLFilter: found irrelevent");
        }
      }
      out = new Outlink[tempOutlinks.size()];
      for (int i = 0; i < tempOutlinks.size(); i++) {
        out[i] = tempOutlinks.get(i);
      }
      ParseData parseData = new ParseData(status, title, out, parse.getData()
          .getContentMeta(), parse.getData().getParseMeta());

      // replace original parse obj with new one
      parseResult.put(content.getUrl(), new ParseText(text), parseData);

    } else {
      LOG.info("ModelURLFilter: Page found relevent:: " + url);
    }

    // TODO Auto-generated method stub

    return parseResult;
  }

}

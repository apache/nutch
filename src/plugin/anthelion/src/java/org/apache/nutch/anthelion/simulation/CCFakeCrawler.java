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
package org.apache.nutch.anthelion.simulation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.nutch.anthelion.framework.AnthProcessor;
import org.apache.nutch.anthelion.models.AnthURL;

import de.uni_mannheim.informatik.dws.dwslib.util.InputUtil;

/**
 * This class is primary used to simulate a crawler. The input is a list of URLs
 * with features and already labels, so the feedback will be instant.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class CCFakeCrawler {

  private Properties configProp = new Properties();

  private TreeMap<Long, Integer> mapper = new TreeMap<Long, Integer>();
  // private TreeMap<Integer, String> idUrlMap = new TreeMap<Integer,
  // String>();
  @SuppressWarnings("unused")
  private String logFileName = null;
  private String[] urlArray = new String[5553007];
  // private HashMap<String, Integer> urlIdMap = new HashMap<String,
  // Integer>();
  @SuppressWarnings("unchecked")
  List<Integer>[] linkArrayList = new List[5553007];
  // private TreeMap<Integer, Set<Integer>> idLinkMap = new TreeMap<Integer,
  // Set<Integer>>();
  private TreeSet<Integer> semIds = new TreeSet<Integer>();
  // make sure we do not crawl one id multiple times
  private TreeSet<Integer> sieve = new TreeSet<Integer>();
  // output list which the crawler processes
  public Queue<AnthURL> readyUrls = new LinkedList<AnthURL>();
  // input queue
  public Queue<AnthURL> newUrls = new LinkedList<AnthURL>();
  // crawled URLs waiting for semantic extraction
  public Set<Integer> semExtraction = new HashSet<Integer>();
  // AnthProcessor
  public AnthProcessor processor;
  // crawler
  public Thread crawler;
  // init state
  public boolean init = false;

  /**
   * Initiates the CommonCrawl Fake Crawler.
   * 
   * @param indexFile
   *            File including the URL and Index of all crawlable URLs
   * @param linkFile
   *            File including all links from one URL to another, using sparse
   *            representation
   * @param semFile
   *            File including IDs of URLs which are semantic
   * @throws IOException
   *             Crashes if there is an error while reading the files
   */
  private void initCrawler(String indexFile, String linkFile, String semFile,
      String propFile, String logFile) throws IOException {

    InputStream in = new FileInputStream(propFile);
    try {
      configProp.load(in);
      System.out.println("Properties loaded.");
    } catch (IOException e) {
      System.out.println("Could not load properties.");
      e.printStackTrace();
      System.exit(0);
    }

    this.logFileName = logFile;
    System.out.println("Loading id - URL map.");
    BufferedReader br = InputUtil.getBufferedReader(new File(indexFile));
    int cnt = 0;
    while (br.ready()) {
      String line = br.readLine();
      String tok[] = line.split("\t");
      Long id = Long.parseLong(tok[0]);
      cnt++;
      mapper.put(id, cnt);
      urlArray[cnt] = tok[1];
      // idUrlMap.put(cnt, tok[1]);
      // urlIdMap.put(tok[1], cnt);
    }
    br.close();
    System.out.println("Read " + mapper.size() + " indexes.");
    System.out.println("Reading id - id link map.");
    br = InputUtil.getBufferedReader(new File(linkFile));
    int lCnt = 0;
    while (br.ready()) {
      if (++lCnt % 100000 == 0) {
        // System.out.println("... read " + lCnt + " Urls with links.");
      }
      String line = br.readLine();
      String tok[] = line.split("\t");
      List<Integer> ids = new ArrayList<Integer>();
      for (int i = 1; i < tok.length; i++) {
        ids.add(mapper.get(Long.parseLong(tok[i])));
      }
      linkArrayList[mapper.get(Long.parseLong(tok[0]))] = ids;
      // idLinkMap.put(mapper.get(Long.parseLong(tok[0])), ids);
    }
    br.close();
    System.out.println("Read " + lCnt + " urls with outgoing links.");
    System.out.println("Reading sem id set.");
    br = InputUtil.getBufferedReader(new File(semFile));
    while (br.ready()) {
      String line = br.readLine();
      String tok[] = line.split("\t");
      Integer id = mapper.get(Long.parseLong(tok[0]));
      if (id != null) {
        semIds.add(id);
      }
    }
    br.close();
    System.out.println("Read " + semIds.size() + " semantic ids.");
    System.out.println("Initializing processor");
    processor = new AnthProcessor(newUrls, readyUrls, configProp, logFile);
    System.out.println("Initializing crawler");
    crawler = new Thread(new CrawlerThread(), "CCFakeCrawler crawler");
  }

  private void initializeClassifier(String initClassifierFile)
      throws IOException {
    if (processor == null) {
      System.out
      .println("Could not initialize Classifier as processor is not ready.");
      return;
    }
    BufferedReader br = InputUtil.getBufferedReader(new File(
        initClassifierFile));
    List<AnthURL> initList = new ArrayList<AnthURL>();
    while (br.ready()) {
      String line = br.readLine();
      // format: id url sem semfather
      String[] tok = line.split("\t");
      try {
        initList.add(new AnthURL(Long.parseLong(tok[0]),
            new URI(tok[1]), Boolean.parseBoolean(tok[3]), !Boolean
            .parseBoolean(tok[3]), false, false, Boolean
            .parseBoolean(tok[2])));
      } catch (URISyntaxException uriE) {
        System.out.println("Could not create proper URI from: "
            + tok[1] + ". Skipping.");
      }
    }
    processor.initiateClassifier(initList);
  }

  @SuppressWarnings("deprecation")
  public static void main(String[] args) {

    System.out
    .println("Start with arguments: <IndexFile> <LinkFile> <SemFile> <PropFile> <StatLogFile> (<InitClassifierFile>)");
    CCFakeCrawler cc = new CCFakeCrawler();

    // open up standard input
    BufferedReader input = new BufferedReader(new InputStreamReader(
        System.in));
    boolean init = false;
    boolean anthStarted = false;
    while (true) {
      String line;
      try {
        System.out
        .println("What do you want to do: init, sample, start, crawl, stop, status, exit?");
        line = input.readLine();
        if (line != null && !line.equals("")) {
          if (line.equals("init")) {
            if (init) {
              System.out.println("Already initialized.");
              continue;
            }
            try {
              cc.initCrawler(args[0], args[1], args[2], args[3],
                  args[4]);
              cc.feedInitialSeeds();
              if (args.length > 5) {
                cc.initializeClassifier(args[5]);
              }
              init = true;
            } catch (IOException e) {
              e.printStackTrace();
              System.out.println("Could not initiate.");
            }
          } else if (line.equals("sample")) {
            if (!init) {
              System.out
              .println("Crawler not initiated, please init first.");
              continue;
            }
            cc.createRandomSample();
          } else if (line.equals("start")) {
            if (!init) {
              System.out
              .println("Crawler not initiated, please init first.");
              continue;
            }
            // start processor
            cc.processor.start();
            anthStarted = true;
            System.out.println("... started processor.");
          } else if (line.equals("crawl")) {
            if (!anthStarted) {
              System.out
              .println("Anthelion not started, please start first.");
              continue;
            }
            // start crawler
            cc.crawler.start();
            System.out.println("... started crawler.");
          } else if (line.equals("stop")) {

            if (!init) {
              System.out
              .println("Crawler not initiated, please init first.");
              continue;
            }
            cc.crawler.stop();
            cc.processor.stop();
            System.out
            .println("... stopped processor and crawler.");
          } else if (line.equals("status")) {
            if (!init) {
              System.out
              .println("Crawler not initiated, please init first.");
              continue;
            }
            cc.processor.getStatus();
            System.out.println("CCFakeCrawlerThread: "
                + cc.crawler.getState());
          } else if (line.equals("exit")) {
            return;
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }

  private void feedInitialSeeds() {
    // dmoz seed

    try {
      boolean sem = semIds.contains(mapper.get(96439052l)) ? true : false;
      readyUrls.add(new AnthURL((long) mapper.get(96439052l), new URI(
          urlArray[mapper.get(96439052l)]), sem, !sem, false, false,
          sem));
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  /**
   * Creates a random sample of the data and outputs it to be feed later into
   * the crawler to initialize the classifier.
   * 
   * @param ratio
   * @throws IOException
   */
  private void createRandomSample() throws IOException {
    BufferedReader input = new BufferedReader(new InputStreamReader(
        System.in));
    String line = "";
    System.out
    .println("Please set sample size (as int value) of the sample to be created:");
    line = input.readLine();
    int sampleSize = Integer.parseInt(line);
    System.out.println("Please set name of output file:");
    String outputFile = input.readLine();
    BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
    Random rnd = new Random();
    Integer id = rnd.nextInt(urlArray.length);
    TreeSet<Integer> selectedIds = new TreeSet<Integer>();
    System.out.println("Creating sample ...");
    for (int i = 0; i < sampleSize; i++) {

      while (selectedIds.contains(id)) {
        id = rnd.nextInt(urlArray.length);
      }
      selectedIds.add(id);
      // format: id url sem semfather
      bw.write(id + "\t" + urlArray[id] + "\t"
          + (semIds.contains(id) ? "true" : "false") + "\t" + "false"
          + "\n");
    }
    bw.close();
    System.out.println("... created sample.");
  }

  public class CrawlerThread implements Runnable {

    private boolean run;
    // this is default.
    private int crawlerWaitingTime = 500;

    public void switchOf() {
      run = false;
    }

    @Override
    public void run() {
      // check the speed
      if (configProp.getProperty("crawler.processtime") != null
          && !configProp.getProperty("crawler.waitingtime")
          .equals("")) {
        crawlerWaitingTime = Integer.parseInt(configProp
            .getProperty("crawler.processtime"));
      }
      run = true;
      while (run) {
        AnthURL anthURL = readyUrls.poll();
        if (anthURL != null) {
          // get id
          Integer urlId = (int) anthURL.id;
          // push feedback about crawled url
          boolean sem = semIds.contains(urlId) ? true : false;
          processor.addFeedback(anthURL.uri, sem);
          // get links
          List<Integer> ids = linkArrayList[urlId];
          if (ids != null) {
            for (Integer id : ids) {
              if (sieve.add(id)) {
                try {
                  boolean oSem = semIds.contains(id) ? true
                      : false;
                  AnthURL aurl = new AnthURL((long) id,
                      new URI(urlArray[id]), sem, !sem,
                      false, false, oSem);
                  if (aurl.getHost() == null) {
                    System.out
                    .println("Host could not be calculated from "
                        + aurl.uri.toString());
                    continue;
                  }
                  newUrls.add(aurl);
                } catch (URISyntaxException e) {
                  System.out
                  .println("URI "
                      + urlArray[id]
                          + " is not conform with specification.");
                }
              }
            }
          }
        } else {
          // nothing to do so sleep a little bit
          try {
            Thread.sleep(crawlerWaitingTime);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }

    }

  }

}

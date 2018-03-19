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
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.TreeMap;
import java.util.TreeSet;

import de.uni_mannheim.informatik.dws.dwslib.util.InputUtil;

/**
 * Simulation of a BFS crawler working on the Common Crawl Data of 2012.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class BFSCrawler {

  private static long INITIALSEED = 96439052l;

  private TreeMap<Long, Integer> mapper = new TreeMap<Long, Integer>();

  private String logFileName = null;

  @SuppressWarnings("unchecked")
  List<Integer>[] linkArrayList = new List[5553007];
  private TreeSet<Integer> semIds = new TreeSet<Integer>();
  // list of URLs to crawl
  private Queue<Integer> readyURLs = new LinkedList<Integer>();
  // make sure we do not crawl one id multiple times, also called sieve
  private TreeSet<Integer> sieve = new TreeSet<Integer>();

  // output list which the crawler processes

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
      String logFile) throws IOException {

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
    }
    br.close();
    System.out.println("Read " + mapper.size() + " indexes.");
    System.out.println("Reading id - id link map.");
    br = InputUtil.getBufferedReader(new File(linkFile));
    int lCnt = 0;
    while (br.ready()) {
      if (++lCnt % 100000 == 0) {
        System.out.println("... read " + lCnt + " Urls with links.");
      }
      String line = br.readLine();
      String tok[] = line.split("\t");
      List<Integer> ids = new ArrayList<Integer>();
      for (int i = 1; i < tok.length; i++) {
        ids.add(mapper.get(Long.parseLong(tok[i])));
      }
      linkArrayList[mapper.get(Long.parseLong(tok[0]))] = ids;
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
  }

  public void run() throws IOException {
    long sem = 0;
    long nonsem = 0;
    long cnt = 0;
    // create logger
    BufferedWriter bf = new BufferedWriter(new FileWriter(logFileName));
    // we init the whole process by adding the initial seed
    sieve.add(mapper.get(INITIALSEED));
    readyURLs.add(mapper.get(INITIALSEED));
    while (!readyURLs.isEmpty()) {
      if (++cnt % 100000 == 0) {
        System.out.println("Crawled " + cnt + " URLs.");
      }
      if (cnt % 100 == 0) {
        bf.write(cnt + "\t" + sem + "\t" + nonsem + "\n");
      }
      // first we get the next element
      Integer currentCrawledPage = readyURLs.poll();
      // we get the list of URLs which we found links - but we do not know
      // the correct order so we shuffle
      List<Integer> linkList = linkArrayList[currentCrawledPage];
      if (linkList != null && !linkList.isEmpty()) {
        Collections.shuffle(linkList);
        for (int link : linkList) {
          if (sieve.add(link)) {
            readyURLs.add(link);
          }
        }
      }
      if (semIds.contains(currentCrawledPage)) {
        sem++;
      } else {
        nonsem++;
      }
    }
    bf.close();
  }

  public static void main(String[] args) throws IOException {
    if (args == null || args.length != 4) {
      System.out
      .println("USAGE: BFSCrawler <INDEXFILE> <LINKFILE> <SEMFILE> <LOGFILE>");
      return;
    }
    BFSCrawler bfs = new BFSCrawler();
    bfs.initCrawler(args[0], args[1], args[2], args[3]);
    bfs.run();
  }

}

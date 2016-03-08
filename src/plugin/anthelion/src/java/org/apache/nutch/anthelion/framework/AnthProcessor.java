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
package org.apache.nutch.anthelion.framework;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.nutch.anthelion.models.AnthHost;
import org.apache.nutch.anthelion.models.AnthURL;
import org.apache.nutch.anthelion.models.ClassificationMode;
import org.apache.nutch.anthelion.models.banditfunction.DomainValueFunction;

/**
 * Main class of the Anthelion module. This class includes all the necessary
 * components to manipulate an incoming {@link AnthURL} list, reorder the list
 * and push it to a given {@link AnthURL} output list. Within the process, the
 * URLs will be attached to the hosts and a bandit approach will always select
 * the next host, URLs should be crawled from. Within the Domain-Entity a
 * {@link PriorityQueue} is filled with URLs based on the score, calculated by
 * the {@link AnthOnlineClassifier}.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class AnthProcessor {

  private BufferedWriter logFileWriter;
  private boolean writeLog;

  protected Queue<AnthURL> inputList;
  protected Queue<AnthURL> outputList;

  private boolean useStaticClassifier = false;

  protected ConcurrentHashMap<String, AnthHost> knownDomains = new ConcurrentHashMap<String, AnthHost>();
  private Thread banditThread;
  private Thread pusherThread;
  private Thread pullerThread;
  private Thread statusThread;
  private UrlPusher pusher;
  private UrlPuller puller;
  private StatusPrinter statusViewer;
  private AnthBandit bandit;

  protected AnthOnlineClassifier onlineLearner;
  protected ArrayBlockingQueue<AnthHost> queuedDomains;

  public AnthProcessor(Queue<AnthURL> inputList, Queue<AnthURL> outputList,
      Properties prop) {
    new AnthProcessor(inputList, outputList, prop, null);
  }

  /**
   * Use to initialize crawler or add additional feedback. Will immediately
   * start the learning process.
   * 
   * @param list
   *            of {@link AnthURL}s which should be used to learn.
   */
  public void initiateClassifier(List<AnthURL> list) {
    onlineLearner.initialize(list);
  }

  public AnthProcessor(Queue<AnthURL> inputList, Queue<AnthURL> outputList,
      Properties prop, String logFile) {

    if (prop.getProperty("classifier.static") != null) {
      useStaticClassifier = Boolean.parseBoolean(prop
          .getProperty("classifier.static"));
    }
    if (prop.get("bandit.lambdadecay") == null) {
      bandit = new AnthBandit(Double.parseDouble(prop
          .getProperty("bandit.lambda")), Integer.parseInt(prop
              .getProperty("domain.known.min")), Integer.parseInt(prop
                  .getProperty("domain.queue.offertime")), this);
    } else {
      bandit = new AnthBandit(
          Double.parseDouble(prop.getProperty("bandit.lambda")),

          Integer.parseInt(prop.getProperty("domain.known.min")),
          Integer.parseInt(prop.getProperty("domain.queue.offertime")),
          this, Boolean.parseBoolean(prop
              .getProperty("bandit.lambdadecay")), Integer
          .parseInt(prop
              .getProperty("bandit.lambdadecayvalue")));
    }
    pusher = new UrlPusher(this, ClassificationMode.valueOf(prop
        .getProperty("classifier.mode")) == ClassificationMode.OnDemand);
    try {
      puller = new UrlPuller(
          this,
          Integer.parseInt(prop.getProperty("inputlist.size")),
          ClassificationMode.valueOf(prop
              .getProperty("classifier.mode")) == ClassificationMode.OnExplore,
              (DomainValueFunction) Class.forName(
                  prop.getProperty("domain.value.function"))
              .newInstance());
    } catch (NumberFormatException | InstantiationException
        | IllegalAccessException | ClassNotFoundException e1) {
      System.out.println("Could not find HostValueFunction");
      e1.printStackTrace();
      System.exit(0);
    }
    if (logFile != null) {
      try {
        this.logFileWriter = new BufferedWriter(new FileWriter(
            new File(logFile)));
        writeLog = true;
      } catch (IOException e) {
        System.out.println("Could not create logWriter");
        writeLog = false;
      }
    } else {
      // just for readability
      writeLog = false;
    }
    statusViewer = new StatusPrinter();
    // setting references
    this.inputList = inputList;
    this.outputList = outputList;
    // initializing
    queuedDomains = new ArrayBlockingQueue<AnthHost>(Integer.parseInt(prop
        .getProperty("domain.queue.size")));
    banditThread = new Thread(bandit, "Anthelion Bandit");
    pusherThread = new Thread(pusher, "Anthelion URL Pusher");
    pullerThread = new Thread(puller, "Anthelion URL Puller");
    statusThread = new Thread(statusViewer, "Anthelion Status Viewer");
    onlineLearner = new AnthOnlineClassifier(
        prop.getProperty("classifier.name"),
        prop.getProperty("classifier.options"),
        Integer.parseInt(prop.getProperty("classifier.hashtricksize")),
        Integer.parseInt(prop.getProperty("classifier.learn.batchsize")));
  }

  /**
   * Start the threads.
   */
  public void start() {
    statusThread.start();
    System.out.println("Started Status Viewer");
    pullerThread.start();
    System.out.println("Started Url Puller");
    banditThread.start();
    System.out.println("Started Bandit");
    pusherThread.start();
    System.out.println("Started Url Pusher");
  }

  /**
   * Stop the threads.
   */
  public void stop() {
    puller.switchOf();
    System.out.println("Switched of Url Puller");
    bandit.switchOf();
    System.out.println("Switched of Bandit");
    pusher.switchOf();
    System.out.println("Switched of Url Pusher");
    statusViewer.switchOf();
    System.out.println("Switched of Status Viewer");
  }

  /**
   * Check the status of the threads.
   */
  public void getStatus() {
    System.out.println("Status Url Puller: " + pullerThread.getState());
    System.out.println("Status Bandit: " + banditThread.getState());
    System.out.println("Status Url Pusher: " + pusherThread.getState());
  }

  /**
   * Switch of the URL Puller to empty the internal queue.
   */
  public void runEmpty() {
    puller.switchOf();
    System.out.println("Switched of Url Puller");
  }

  /**
   * Push crawled feedback back into the system.
   * 
   * @param url
   *            URL String - as we assume the data structure used by the
   *            crawler is different than the internal data structure.
   * @param sem
   *            if the URL included structured data or not.
   * @throws URISyntaxException
   *             if the URL String is not valid based on the {@link URI}
   *             specifications.
   */
  public void addFeedback(String url, boolean sem) throws URISyntaxException {
    // we need to give feedback to the classifier and the domains itself
    URI uri = new URI(url);
    addFeedback(uri, sem);
  }

  public void addFeedback(URI uri, boolean sem) {
    AnthHost domain = knownDomains.get((uri.getHost() != null ? uri
        .getHost() : AnthURL.UNKNOWNHOST));
    if (domain == null) {
      return;
    }
    AnthURL aurl = domain.returnFeedback(uri, sem);
    if (aurl == null) {
      // there is one way this could happen - if the URL was two times in
      // the process. If the time between the two appearance is great
      // enough Anthelion will also push the URL multiple times as
      // feedback.
      return;
    }
    // learn / update classifier if not static
    if (!useStaticClassifier) {
      onlineLearner.pushFeedback(aurl);
    }

  }

  private class StatusPrinter implements Runnable {

    public StatusPrinter() {
      if (writeLog) {
        writeLogFileHeader();
      }
    }

    protected boolean run;

    public void switchOf() {
      run = false;
    }

    private void writeLogFileHeader() {
      if (writeLog) {
        String header = "LogTime\tInputListSize\tUrlsPulled\tGoodUrlsPulled\tOutputListSize\tPushedUrls\tPusherProcessingTime\tPushedGoodUrls\tPushedBadUrls\tKnownDomains\tDomainsInQueue\tReadyUrls\tReadyDomains\tUrlsWaitingForFeedback\tBanditProcessingTime\tBanditArmsPulled\tBanditGoodArmsPulled\tBanditBadArmsPulled\tClassifierRight\tClassifierTotal\tClassifiedRightsPushed\n";
        try {
          logFileWriter.write(header);
          logFileWriter.flush();
        } catch (IOException e) {
          System.out.println("LogFile is not ready to be written.");
          writeLog = false;
        }
      } else {
        System.out.println("LogFile is not ready to be written.");
      }
    }

    @Override
    public void run() {
      run = true;
      while (run) {
        Iterator<Map.Entry<String, AnthHost>> iter = knownDomains
            .entrySet().iterator();
        int rdyDomains = 0;
        int readyUrls = 0;
        int feedback = 0;
        int processed = 0;
        int right = 0;
        int banditGood = 0;
        int banditBad = 0;
        while (iter.hasNext()) {
          Map.Entry<String, AnthHost> pairs = (Map.Entry<String, AnthHost>) iter
              .next();
          if (pairs.getValue().rdyToEnqueue()) {
            rdyDomains++;
          }
          readyUrls += pairs.getValue().getReadyUrlsSize();
          feedback += pairs.getValue().getAwaitingFeedbackSize();
          processed += pairs.getValue().visitedUrls;
          right += pairs.getValue().predictedRight;
          banditGood += pairs.getValue().goodUrls;
          banditBad += pairs.getValue().badUrls;
        }
        StringBuilder sb = new StringBuilder();
        if (writeLog) {
          // date
          sb.append(new Date());
          sb.append("\t");
          // size of input list
          sb.append(inputList.size());
          sb.append("\t");
          // number of pulled urls
          sb.append(puller.pulledUrl);
          sb.append("\t");
          // number of good pulled urls
          sb.append(puller.goodPulledUrl);
          sb.append("\t");
          // size of output list
          sb.append(outputList.size());
          sb.append("\t");
          // number of pushed URLs
          sb.append(pusher.pushedUrl);
          sb.append("\t");
          // processing time of pusher
          sb.append(pusher.processingTime);
          sb.append("\t");
          // good pushed
          sb.append(pusher.good);
          sb.append("\t");
          // bad pushed
          sb.append(pusher.bad);
          sb.append("\t");
          // number of known domains
          sb.append(knownDomains.size());
          sb.append("\t");
          // queue size of domains to be choosen from
          sb.append(queuedDomains.size());
          sb.append("\t");
          // number of urls ready to be taken by pusher
          sb.append(readyUrls);
          sb.append("\t");
          // ready domains
          sb.append(rdyDomains);
          sb.append("\t");
          // waiting for feedback
          sb.append(feedback);
          sb.append("\t");
          // bandit processing time
          sb.append(bandit.processingTime);
          sb.append("\t");
          // number of arms pulled by bandit
          sb.append(bandit.armsPulled);
          sb.append("\t");
          sb.append(banditGood);
          sb.append("\t");
          sb.append(banditBad);
          sb.append("\t");
          // right classified
          sb.append(right);
          sb.append("\t");
          // classified in total
          sb.append(processed);
          sb.append("\t");
          // predicted right pushed
          sb.append(pusher.predictedRight);
          sb.append("\n");
          try {
            logFileWriter.write(sb.toString());
            logFileWriter.flush();
          } catch (IOException e) {
            System.out
            .println("Could not write log. Switch to console.");
            writeLog = false;
          }
        } else {
          // status about the queues
          sb.append("\n----------------------------------------------------\n");
          sb.append(new Date());
          sb.append(" Status Viewer says: ");
          sb.append("\nInput queue: ");
          sb.append(inputList.size());
          sb.append("\nPulled URLs (good): ");
          sb.append(puller.pulledUrl);
          sb.append(" (");
          sb.append(puller.goodPulledUrl);
          sb.append(")\nOutput queue: ");
          sb.append(outputList.size());
          sb.append("\nPushed URLs: ");
          sb.append(pusher.pushedUrl);
          sb.append("\nKnown Domains: ");
          sb.append(knownDomains.size());
          sb.append("\nQueued Domains: ");
          sb.append(queuedDomains.size());
          sb.append("\nQueued Urls: ");
          sb.append(readyUrls);
          sb.append(" with ");
          sb.append(rdyDomains);
          sb.append(" domains beeing ready.");
          sb.append("\nWaiting for feedback from URLs: ");
          sb.append(feedback);
          sb.append("\nBandit Processing time (ms): ");
          sb.append((double) bandit.processingTime
              / bandit.armsPulled);
          sb.append(" after pulling ");
          sb.append(bandit.armsPulled);
          sb.append(" arms.");
          // processing rate
          sb.append("\nAvg Time to pull new URL from queue (ms): ");
          sb.append((double) pusher.processingTime / pusher.pushedUrl);
          sb.append("\nCrawling Ratio (good/bad): ");
          long good = pusher.good;
          long bad = pusher.bad;
          sb.append((double) good / bad);
          sb.append(" (");
          sb.append(good);
          sb.append("/");
          sb.append(bad);
          sb.append(")");
          // accuracy
          sb.append("\nClassifier Accuracy: ");
          sb.append((double) right / processed);
          sb.append(" (");
          sb.append(right);
          sb.append("/");
          sb.append(processed);
          sb.append(")");

          sb.append("\n");
          System.out.println(sb.toString());
        }
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

      }
    }
  }
}

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

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.nutch.anthelion.models.AnthHost;

/**
 * Domain/PLD selection process based on a bandit.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class AnthBandit implements Runnable {

  AnthProcessor processor;
  Random rnd;

  private double lambda;
  private boolean run;
  protected long armsPulled;
  protected long processingTime;
  private int minKnownHosts;
  private int domainQueueOfferTime;
  private boolean lambdaDecay = false;

  /**
   * This values determine the ratio of the decay (whenever
   * {@link AnthBandit#lambdaDecay} is true) based on the following function:
   * lambda = OriginalLambda * 1/((int) armsPulled / lambdaDecayValue) + 1
   */
  private int lambdaDecayValue;

  public AnthBandit(double lambda, int minKnownDomains,
      int domainQueueOfferTime, AnthProcessor p) {
    this(lambda, minKnownDomains, domainQueueOfferTime, p, false, 1);
  }

  /**
   * Initializes a Bandit selector for domains.
   * 
   * @param lambda
   *            percentage of random selections
   * @param domainValueFunction
   *            function to calculate the value of a domain
   * @param minKnownDomains
   *            minimum number of known domains till bandit start with
   *            selection
   * @param domainQueueOfferTime
   *            time span the bandit tries to add a domain to be crawled.
   * @param p
   */
  public AnthBandit(double lambda, int minKnownDomains,
      int domainQueueOfferTime, AnthProcessor p, boolean lambdaDecay,
      int lambdaDecayValue) {
    this.lambda = lambda;
    this.lambdaDecay = lambdaDecay;
    this.lambdaDecayValue = lambdaDecayValue;
    this.domainQueueOfferTime = domainQueueOfferTime;
    this.minKnownHosts = minKnownDomains;
    this.processor = p;
    this.rnd = new Random();
  }

  public void switchOf() {
    run = false;
  }

  /**
   * gets the next domain out of the queue. Just a references as all domains
   * stay in the Bandit.
   * 
   * @throws InterruptedException
   */
  private void getNextItem() throws InterruptedException {
    // we have to wait till there is at least the minimum number of domain
    while (processor.knownDomains.size() < minKnownHosts) {
      System.out
      .println("Bandit says: Not enought domains discovered yet.");
      Thread.sleep(5000);
    }
    long time = new Date().getTime();
    // we select some domain random
    // this will also make the thread wait if there is nothing to be found.
    AnthHost domain = null;
    double currentDomainScore = 0.0;
    double tmp = 0.0;
    int tries = 0;

    double curLambda = lambda;
    if (lambdaDecay) {
      curLambda = lambda * (1 / ((armsPulled / lambdaDecayValue) + 1));
    }

    if (!(rnd.nextDouble() < curLambda)) {
      // we take the score
      Iterator<Map.Entry<String, AnthHost>> iter = processor.knownDomains
          .entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<String, AnthHost> pairs = (Map.Entry<String, AnthHost>) iter
            .next();
        AnthHost d = pairs.getValue();

        if (d.rdyToEnqueue()) {
          if (domain == null || currentDomainScore < d.getScore()) {
            domain = d;
            currentDomainScore = tmp;
          }
        }
      }
    } else {
      List<AnthHost> domains = new ArrayList<AnthHost>(
          processor.knownDomains.values());
      while (domain == null || tries < domains.size()) {
        domain = domains.get(rnd.nextInt(domains.size()));
        tries++;
        if (!domain.rdyToEnqueue()) {
          domain = null;
        }
      }
    }
    if (domain == null) {
      System.out.println("Did not find a domain to process. Sleeping.");
      Thread.sleep(1000);
    } else {

      try {
        processor.queuedDomains.offer(domain, domainQueueOfferTime,
            TimeUnit.SECONDS);
        domain.enqueue();
        processingTime += new Date().getTime() - time;
        if (++armsPulled % 10000 == 0) {
          System.out
          .println("Bandit says: Average processing time is "
              + (double) processingTime / armsPulled
              + " ms after pulling " + armsPulled
              + " arms.");
        }
        ;
      } catch (InterruptedException e) {
        wait(1000);
        System.out
        .println("Bandit says: Domain queue seems to be full. Waiting.");
      }
    }

  }

  @Override
  public void run() {
    run = true;
    while (run) {
      try {
        getNextItem();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }

}

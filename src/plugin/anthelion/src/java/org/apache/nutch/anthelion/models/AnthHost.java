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
package org.apache.nutch.anthelion.models;

import java.net.URI;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nutch.anthelion.framework.AnthOnlineClassifier;
import org.apache.nutch.anthelion.framework.AnthProcessor;
import org.apache.nutch.anthelion.models.banditfunction.DomainValueFunction;

/**
 * Class including all data necessary to represent a domain in Anthelion.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class AnthHost {

  /**
   * The name of the {@link AnthHost}
   */
  public String host;

  /**
   * This score (introduced in version 0.0.3) should be updated internally to
   * reduce runtime.
   */
  private double score;

  private DomainValueFunction valueFunction;

  public double getScore() {
    if (scoreUpdateNeeded) {
      score = valueFunction.getDomainValue(this);
    }
    return score;
  }

  /**
   * Reclassifies all known URLs with a given classifier
   * 
   * @param classifier
   *            given {@link AnthOnlineClassifier}
   */
  public void classify(AnthOnlineClassifier classifier) {
    for (AnthURL aurl : readyUrls) {
      classifier.classifyUrl(aurl);
    }
  }

  /**
   * Indicates if the score needs to be updated or not.
   */
  private boolean scoreUpdateNeeded;

  /**
   * The list of {@link AnthURL} belonging to this {@link AnthHost} which
   * could potentially be crawled.
   */
  private PriorityQueue<AnthURL> readyUrls = new PriorityQueue<AnthURL>();
  /**
   * Internal counter of URLs belonging to this {@link AnthHost} which already
   * have been crawled.
   */
  public long visitedUrls = 0;

  /**
   * Internal counter of URLs belonging to this {@link AnthHost} for which the
   * {@link AnthOnlineClassifier} predicted correctly.
   */
  public long predictedRight = 0;

  /**
   * Internal counter of number of good (what ever this means) URLs belonging
   * to this {@link AnthHost} and having been already crawled due to being
   * pulled by the bandit.
   */
  public long goodUrls = 0;

  /**
   * Internal counter of number of bad (what ever this means) URLs belonging
   * to this {@link AnthHost} and have been already crawled due to being
   * pulled by the bandit.
   */
  public long badUrls = 0;
  /**
   * Counter for the number of times this instance is enqueued in the
   * {@link AnthProcessor#queuedDomains}
   */
  public AtomicLong enqueued = new AtomicLong(0);

  /**
   * {@link AnthURL} for which this {@link AnthHost} awaits feedback.
   */
  private HashMap<String, AnthURL> awaitingFeedback = new HashMap<String, AnthURL>();

  /**
   * Creates a new instance of {@link AnthHost} using the domain name.
   * 
   * @param host
   *            the host name.
   */
  public AnthHost(String host, DomainValueFunction dvf) {
    this.host = host;
    this.valueFunction = dvf;
  }

  /**
   * Increase the count of this {@link AnthHost} when enqueued.
   */
  public void enqueue() {
    enqueued.incrementAndGet();
  }

  public void enqueue(AnthURL url) {
    readyUrls.add(url);
    if (valueFunction
        .getNecessities(HostValueUpdateNecessity.ON_QUEUE_CHANGE)) {
      scoreUpdateNeeded = true;
    }
  }

  /**
   * Decrease the count of this {@link AnthHost} when dequeued.
   */
  public void dequeue() {
    enqueued.decrementAndGet();
  }

  /**
   * Peeks the next URL in the {@link AnthHost#readyUrls}.
   * 
   * @return
   */
  public AnthURL peekNextURL() {
    return readyUrls.peek();
  }

  /**
   * Update score based on visited {@link AnthURL}. At the moment its a binary
   * (sem or non-sem) feedback.
   * 
   * @param url
   *            the {@link URI} which was crawled and the feedback was found.
   */
  public AnthURL returnFeedback(URI uri, boolean sem) {
    AnthURL url = awaitingFeedback.remove(uri.toString());
    if (url != null) {
      url.sem = sem;
      if (url.prediction > 0 && url.sem) {
        predictedRight++;
      } else if (url.prediction < 0 && !url.sem) {
        predictedRight++;
      }
      // increase counter
      if (url.sem) {
        goodUrls++;
      } else {
        badUrls++;
      }
      visitedUrls++;
      if (valueFunction
          .getNecessities(HostValueUpdateNecessity.ON_STAT_CHANGE)) {
        scoreUpdateNeeded = true;
      }
    } else {
      // log something
    }
    return url;
  }

  /**
   * make sure it is not chosen a second time
   * 
   * @param url
   *            the {@link AnthURL} to be removed from the domain
   */
  public void dequeue(AnthURL url) {
    readyUrls.remove(url);
    if (valueFunction
        .getNecessities(HostValueUpdateNecessity.ON_QUEUE_CHANGE)) {
      scoreUpdateNeeded = true;
    }
  }

  /**
   * Checks weather a {@link AnthHost} can be enqueued or not, by calculating
   * if, at the point the {@link AnthHost} needs to include at least one URL
   * to be crawled is capable of having one. The calculating is based on worth
   * case scenario - so if no new URL is added to the {@link AnthHost}.
   * 
   * @return true if enough URLs are left. False else.
   */
  public boolean rdyToEnqueue() {
    return (readyUrls.size() - enqueued.get() > 0);
  }

  /**
   * Retrieves the next (highest scored) URL from the {@link AnthHost} and
   * adds the {@link AnthURL} to the list of {@link AnthURL}s which are
   * currently put to be crawled but feedback is pending.
   * 
   * @return {@link AnthURL} to be crawled
   */
  public AnthURL getNextUrlToCrawl() {
    if (readyUrls != null && !readyUrls.isEmpty()
        && readyUrls.peek() != null) {
      try {
        AnthURL urlToCrawl = readyUrls.poll();
        awaitingFeedback.put(urlToCrawl.uri.toString(), urlToCrawl);
        if (valueFunction
            .getNecessities(HostValueUpdateNecessity.ON_QUEUE_CHANGE)) {
          scoreUpdateNeeded = true;
        }
        return urlToCrawl;
      } catch (NullPointerException npe) {
        // i do not know why it is crushing here.
        return null;
      }
    } else {
      return null;
    }
  }

  /**
   * Retrieves the k next (highest scored) URLs from the {@link AnthHost} and
   * adds the {@link AnthURL} to the list of {@link AnthURL}s which are put to
   * be crawled but feedback is pending. It is not guaranteed that enough URLs
   * are available. If so, the return values will be null at that index.
   * 
   * @param num
   *            the number of {@link AnthURL} to be retrieved.
   * @return an {@link AnthURL} of lenght num where the lowest index includes
   *         the highest scored URL from the {@link AnthHost}.
   */
  public AnthURL[] getNextUrlToCrawl(int num) {
    AnthURL[] urlsToCrawl = new AnthURL[num];
    for (int i = 0; i < num; i++) {
      urlsToCrawl[i] = getNextUrlToCrawl();
    }
    return urlsToCrawl;
  }

  public int getReadyUrlsSize() {
    return readyUrls.size();
  }

  public int getAwaitingFeedbackSize() {
    return awaitingFeedback.size();
  }

}

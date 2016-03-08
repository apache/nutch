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

import java.util.Date;

import org.apache.nutch.anthelion.models.AnthHost;
import org.apache.nutch.anthelion.models.AnthURL;

/**
 * Class which simply pushs URLs from the sorted list of domains (belonging to the
 * {@link AnthProcessor}) to the real crawler. URLs are removed from the  corresponding domain/host. 
 * {@link UrlPusher} will only push one URL at the time.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 *
 */
public class UrlPusher implements Runnable {

  private AnthProcessor processor;
  private boolean run;
  protected long pushedUrl;
  protected long processingTime;
  protected long good;
  protected long bad;
  protected long predictedRight;
  private boolean classifyOnPush;

  public UrlPusher(AnthProcessor p, boolean classifyOnPush) {
    this.processor = p;
    this.classifyOnPush = classifyOnPush;

  }

  public void switchOf() {
    run = false;
  }

  @Override
  public void run() {
    run = true;
    while (run) {
      if (processor.queuedDomains.peek() != null) {
        long timeStart = new Date().getTime();
        AnthHost nextHost = processor.queuedDomains.poll();
        if (classifyOnPush) {
          nextHost.classify(processor.onlineLearner);
        }
        AnthURL aurl = nextHost.getNextUrlToCrawl();
        if (aurl != null) {
          if (aurl.sem) {
            good++;
            if (aurl.prediction >= 0) {
              predictedRight++;
            }
          } else {
            bad++;
            if (aurl.prediction <= 0) {
              predictedRight++;
            }
          }
          processor.outputList.add(aurl);
          nextHost.dequeue();
          pushedUrl++;
          processingTime += new Date().getTime() - timeStart;
        }
      } else {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

}

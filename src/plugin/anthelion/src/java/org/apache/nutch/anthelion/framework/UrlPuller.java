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

import org.apache.nutch.anthelion.models.AnthHost;
import org.apache.nutch.anthelion.models.AnthURL;
import org.apache.nutch.anthelion.models.banditfunction.DomainValueFunction;

/**
 * Class which simply pulls URLs from a given List (belonging to the
 * {@link AnthProcessor}) and includes them into Anthelion modul. URLs coming
 * from outside are added to the corresponding domain/host. If the host does not
 * exist it is created on the fly. {@link UrlPuller} will only pull one URL at
 * the time.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class UrlPuller implements Runnable {

  private AnthProcessor p;
  private boolean run;
  protected long pulledUrl;
  protected long goodPulledUrl;
  private int minInputListSize;
  private boolean classifyOnPull;
  private DomainValueFunction domainValueFunction;

  public UrlPuller(AnthProcessor p, int minInputListSize,
      boolean classifyOnPull, DomainValueFunction domainValueFunction) {
    this.minInputListSize = minInputListSize;
    this.classifyOnPull = classifyOnPull;
    this.p = p;
    this.domainValueFunction = domainValueFunction;
  }

  public void switchOf() {
    run = false;
  }

  @Override
  public void run() {
    run = true;
    while (run) {
      if (p.inputList.size() > minInputListSize) {
        AnthURL aurl = p.inputList.poll();
        if (aurl != null) {
          if (classifyOnPull) {
            p.onlineLearner.classifyUrl(aurl);
          }
          if (!p.knownDomains.containsKey(aurl.getHost())) {
            p.knownDomains.put(aurl.getHost(),
                new AnthHost(aurl.getHost(),
                    domainValueFunction));
          }
          try {
            p.knownDomains.get(aurl.getHost()).enqueue(aurl);
            if (aurl.sem) {
              goodPulledUrl++;
            }
            pulledUrl++;
          } catch (NullPointerException npe) {
            // clap your hands.
          }
        }
      } else {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

  }
}

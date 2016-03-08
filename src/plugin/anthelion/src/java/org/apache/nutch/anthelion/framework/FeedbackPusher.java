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

import java.util.Queue;

import org.apache.nutch.anthelion.models.AnthURL;

/**
 * Class which based on the configuration pushes feedback to the online
 * classification method used.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class FeedbackPusher implements Runnable {
  public AnthProcessor p;
  public Queue<AnthURL> readyUrls;
  protected boolean run;

  public FeedbackPusher(AnthProcessor p, Queue<AnthURL> readyUrls) {
    super();
    this.p = p;
    this.readyUrls = readyUrls;
  }

  public void switchOf() {
    run = false;
  }

  @Override
  public void run() {
    run = true;
    while (run) {
      AnthURL url = readyUrls.poll();
      if (url != null) {
        p.addFeedback(url.uri, url.sem);

      } else {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

  }
}
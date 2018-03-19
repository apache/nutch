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
package org.apache.nutch.anthelion.models.banditfunction;

import java.util.Properties;

import org.apache.nutch.anthelion.models.AnthHost;
import org.apache.nutch.anthelion.models.HostValueUpdateNecessity;

/**
 * Calculates the score of a domain for the bandit approach based on the
 * following attributes:
 * <ul>
 * <li>Number of times the domain is already enqueued in the domain queue</li>
 * <li>Ration between good URLs and visited URLs</li>
 * <li>Number of outstanding feedbacks for this domain based on the
 * {@link RuntimeConfig#FEEDBACK_PENALTY}</li>
 * </ul>
 * 
 * The function works as follows 1) Check if at least on URLs of the domain was
 * already visited. If not return 0. 2) If not 1) check if the fraction of this
 * domain of the whole domain queue is larger as the
 * {@link RuntimeConfig#MAX_DOMAIN_QUEUE_FRACTION}. If so return 0. 3) If not 2)
 * Calculate the score based on the product out of a) the ratio between goodUrls
 * and visitedUrls, b) the ratio of right predicted URLs (using the classifier)
 * and discounted ratio of URLs which await feedback divided by the number of
 * visited URLs
 * 
 * 
 * For more information see Meusel et al. 2014 @ CIKM'14
 * (http://dws.informatik.uni
 * -mannheim.de/fileadmin/lehrstuehle/ki/pub/Meusel-etal
 * -FocusedCrawlingForStructuredData-CIKM14.pdf)
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class DomainHolisticFunction implements DomainValueFunction {

  private double maxDomainQueueFraction;
  private double missingFeedbackPenalty;
  private int domainQueueSize;
  private double valueAdapter;
  private double unknownDomainValue;

  @Override
  public double getDomainValue(AnthHost domain) {
    if (domain.visitedUrls == 0) {
      return unknownDomainValue;
    } else {
      if (((double) domainQueueSize / (domain.enqueued.get() + 1)) < maxDomainQueueFraction) {
        double goodBadRatio = ((double) domain.goodUrls + valueAdapter)
            / (domain.visitedUrls + valueAdapter);
        double accuracy = ((double) domain.predictedRight + valueAdapter)
            / (domain.visitedUrls + valueAdapter);
        // double penalty = 1
        // - ((double) domain.awaitingFeedback.size() *
        // missingFeedbackPenalty)
        // / domain.visitedUrls;

        return goodBadRatio * accuracy;
      } else {
        // this is the lowest expectable value
        return 0;
      }
    }
  }

  @Override
  public void setProperty(Properties prop) {
    maxDomainQueueFraction = Double.parseDouble(prop
        .getProperty("domain.queue.fraction"));
    missingFeedbackPenalty = Double.parseDouble(prop
        .getProperty("domain.value.function.missingfeedbackpenalty"));
    domainQueueSize = Integer.parseInt(prop
        .getProperty("domain.queue.size"));
    unknownDomainValue = Double.parseDouble(prop
        .getProperty("domain.unkown.value"));
    valueAdapter = Double.parseDouble(prop
        .getProperty("domain.value.function.valueadapter"));
  }

  @Override
  public boolean getNecessities(HostValueUpdateNecessity nec) {
    if (nec == HostValueUpdateNecessity.ON_STAT_CHANGE) {
      return true;
    }
    if (nec == HostValueUpdateNecessity.ON_QUEUE_CHANGE) {
      return true;
    }
    return false;
  }
}

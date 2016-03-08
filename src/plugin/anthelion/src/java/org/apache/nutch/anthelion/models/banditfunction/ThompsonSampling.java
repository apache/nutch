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

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.BetaDistributionImpl;
import org.apache.nutch.anthelion.models.AnthHost;
import org.apache.nutch.anthelion.models.HostValueUpdateNecessity;

/**
 * Baseline function using a BetaDistribution (alpha 1, beta 1) and the mean to
 * determine the next best domain to crawl.
 *   
 * For more information see Meusel et al. 2014 @ CIKM'14
 * (http://dws.informatik.uni
 * -mannheim.de/fileadmin/lehrstuehle/ki/pub/Meusel-etal
 * -FocusedCrawlingForStructuredData-CIKM14.pdf)
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class ThompsonSampling implements DomainValueFunction {

  private int alpha = 1;
  private int beta = 1;

  @Override
  public double getDomainValue(AnthHost domain) {
    BetaDistributionImpl bd = new BetaDistributionImpl(domain.goodUrls
        + alpha, domain.badUrls + beta);
    try {
      return bd.sample();
    } catch (MathException e) {
      System.out
      .println("Could not get random value from BetaDistribution for "
          + domain.host);
      return 0.0;
    }
  }

  @Override
  public void setProperty(Properties prop) {
    // can be ignored
  }

  public static void main(String[] args) throws MathException {
    BetaDistributionImpl bd = new BetaDistributionImpl(10, 100);
    BetaDistributionImpl bd1 = new BetaDistributionImpl(1, 10);
    for (int i = 0; i < 10; i++) {
      System.out.println("" + bd.sample() + " " + bd.getNumericalMean()
      + " " + bd1.sample() + " " + bd1.getNumericalMean());
    }

  }

  @Override
  public boolean getNecessities(HostValueUpdateNecessity nec) {
    if (nec == HostValueUpdateNecessity.ON_STAT_CHANGE) {
      return true;
    }
    if (nec == HostValueUpdateNecessity.ON_QUEUE_CHANGE) {
      return false;
    }
    return false;
  }
}

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
import org.apache.nutch.anthelion.models.AnthURL;
import org.apache.nutch.anthelion.models.HostValueUpdateNecessity;

/**
 * Bandit Function using the absolute number of correct semantic annotated URLs
 * per Domain combined with the Best Score Function (Based on selected
 * Classifier).
 * 
 * For more information see Meusel et al. 2014 @ CIKM'14
 * (http://dws.informatik.uni
 * -mannheim.de/fileadmin/lehrstuehle/ki/pub/Meusel-etal
 * -FocusedCrawlingForStructuredData-CIKM14.pdf)
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class AbsoluteGoodBestScoreFunction implements DomainValueFunction {

  @Override
  public double getDomainValue(AnthHost host) {

    // initially its the minimal double value
    // as this can only happen when something is wrong with the host we want
    // to ignore it
    double out = Double.MIN_VALUE;
    AnthURL url = null;
    if ((url = host.peekNextURL()) != null) {
      try {
        out = url.prediction;
      } catch (Exception e) {
        // just do nothing.
      }
    }
    return out * host.goodUrls;

  }

  @Override
  public void setProperty(Properties prop) {
    // can be ignored
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

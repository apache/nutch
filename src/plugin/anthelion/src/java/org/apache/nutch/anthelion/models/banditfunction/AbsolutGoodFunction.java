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
 * Simple defines the score of a domain as the number of already semantic URLs
 * found.
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
public class AbsolutGoodFunction implements DomainValueFunction {

  @Override
  public void setProperty(Properties prop) {
    // not needed
  }

  @Override
  public double getDomainValue(AnthHost domain) {
    return domain.goodUrls;
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

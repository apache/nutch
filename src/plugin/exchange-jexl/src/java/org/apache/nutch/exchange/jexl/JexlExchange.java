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
package org.apache.nutch.exchange.jexl;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.MapContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.exchange.Exchange;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.util.JexlUtil;

import java.util.Map;

public class JexlExchange implements Exchange {

  private static final String EXPRESSION_KEY = "expr";

  private Configuration conf;

  private Expression expression;

  /**
   * Initializes the internal variables.
   *
   * @param parameters Params from the exchange configuration.
   */
  @Override
  public void open(Map<String, String> parameters) {
    expression = JexlUtil.parseExpression(parameters.get(EXPRESSION_KEY));
  }

  /**
   * Determines if the document must go to the related index writers.
   *
   * @param doc The given document.
   * @return True if the given document match with this exchange. False in other case.
   */
  @Override
  public boolean match(NutchDocument doc) {
    // Create a context and add data
    JexlContext jexlContext = new MapContext();
    jexlContext.set("doc", doc);

    try {
      if (Boolean.TRUE.equals(expression.evaluate(jexlContext))) {
        return true;
      }
    } catch (Exception ignored) {
    }

    return false;
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}

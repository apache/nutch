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
package org.apache.nutch.exchange;

import org.apache.hadoop.conf.Configurable;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.plugin.Pluggable;

import java.util.Map;

public interface Exchange extends Pluggable, Configurable {

  /**
   * The name of the extension point.
   */
  String X_POINT_ID = Exchange.class.getName();

  /**
   * Initializes the internal variables.
   *
   * @param parameters Params from the exchange configuration.
   */
  void open(Map<String, String> parameters);

  /**
   * Determines if the document must go to the related index writers.
   *
   * @param doc The given document.
   * @return True if the given document match with this exchange. False in other case.
   */
  boolean match(NutchDocument doc);
}

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
package org.apache.nutch.keymatch;

import java.util.List;
import java.util.Map;

/**
 * <p>All implementing classes should extend AbstractFilter
 * </p>
 */
public interface KeyMatchFilter {
  
  /**
   * Do filtering for matches
   * @param matches current List of matches
   * @param context the evaluation context
   * @return 
   */
  public KeyMatch[] filter(List matches, Map context);
  
  /**
   * <p>Set the next filter that is processed after this
   * one</p>
   * @param filter the filter to set
   */
  public void setNext(KeyMatchFilter filter);

}

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

package org.apache.nutch.parse;

import org.apache.nutch.net.URLFilter;

/**
 * An abstract class to make more function prototypes of a url filter plugin
 * available in the core Nutch classes
 */
public abstract class ModelURLFilterAbstract implements URLFilter {

  /** Uses text (parse text) to set state of the class */
  public abstract boolean filterParse(String text);

  /**
   * Can be used instead of the generic filter(String url) to be called in any
   * job other than generator of injector, so that the generic function can be
   * short circuited for the generator i.e. the filter won't work in for the
   * generator
   */
  public abstract boolean filterUrl(String url);

  /**
   * Configure the filter once before using the filtering functions, like train
   * the classifier once
   */
  public abstract void configure(String[] args) throws Exception;

}

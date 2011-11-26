/**
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
package org.apache.nutch.urlfilter.api;

import org.apache.nutch.net.*;


/**
 * A generic regular expression rule.
 *
 * @author J&eacute;r&ocirc;me Charron
 */
public abstract class RegexRule {

  private boolean sign;
  private String regex;

  /**
   * Constructs a new regular expression rule.
   *
   * @param sign specifies if this rule must filter-in or filter-out.
   *        A <code>true</code> value means that any url matching this rule
   *        must be accepted, a <code>false</code> value means that any url
   *        matching this rule must be rejected.
   * @param regex is the regular expression used for matching (see
   *        {@link #match(String)} method).
   */
  protected RegexRule(boolean sign, String regex) {
    this.sign = sign;
    this.regex = regex;
  }

  /**
   * Return if this rule is used for filtering-in or out.
   *
   * @return <code>true</code> if any url matching this rule must be accepted,
   *         otherwise <code>false</code>.
   */
  protected boolean accept() { return sign; }
  
  /**
   * Checks if a url matches this rule.
   * @param url is the url to check.
   * @return <code>true</code> if the specified url matches this rule,
   *         otherwise <code>false</code>.
   */
  protected abstract boolean match(String url);

}


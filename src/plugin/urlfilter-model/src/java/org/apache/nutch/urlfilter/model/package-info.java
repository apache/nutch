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

/**
 * URL filter plugin with a two tier architecture for filtering:
 * The filter is called from the parser and looks at the current page that was parsed.
 * Does a Naive Bayes Classification on the text of the page and decides if it is relevant or not.
 * If relevant then let all the outlinks pass, if not then the second check kicks in,
 * which checks for some "hotwords" in the outlink urls itself (from a wordlist provided by the user).
 * If a match then let the outlink pass).
 */
package org.apache.nutch.urlfilter.model;


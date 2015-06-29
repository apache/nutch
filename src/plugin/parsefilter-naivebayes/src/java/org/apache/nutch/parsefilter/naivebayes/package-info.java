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

/**
 * Html Parse filter that classifies the outlinks from the parseresult as
 * relevant or irrelevant based on the parseText's relevancy (using a training
 * file where you can give positive and negative example texts see the
 * description of parsefilter.naivebayes.trainfile) and if found irrelevent
 * it gives the link a second chance if it contains any of the words from the
 * list given in parsefilter.naivebayes.wordlist. CAUTION: Set the
 * parser.timeout to -1 or a bigger value than 30, when using this classifier.
 */
package org.apache.nutch.parsefilter.naivebayes;


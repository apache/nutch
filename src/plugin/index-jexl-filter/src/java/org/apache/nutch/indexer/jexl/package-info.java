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
 * <p>This plugin implements a dynamic indexing filter which uses JEXL 
 * expressions to allow filtering based on the page's metadata 
 * <p>Available primitives in the JEXL context:<ul>
  * <li>status, fetchTime, modifiedTime, retries, interval, score, signature, url, text, title</li></ul>
 * <p>Available objects in the JEXL context:<ul>
 * <li>httpStatus - contains majorCode, minorCode, message</li>
 * <li>documentMeta, contentMeta, parseMeta - contain all the Metadata properties.<br>
 *   Each property value is always an array of Strings (so if you expect one value, use [0])</li>
 * <li>doc - contains all the NutchFields from the NutchDocument.<br>
 *   Each property value is always an array of Objects.</li></ul>
 * 
 */
package org.apache.nutch.indexer.jexl;
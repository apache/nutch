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
 * <p>Metadata Scoring Plugin</p>
 * <p>Propagates Metadata from an injected or outlink url in the crawldb 
 * to the url's different procecssed objects. In moving any metadata 
 * item, you need to copy metadata in three steps:</p>
 * <ul>
 *   <li>Crawldb to content: Copy a metadata entry stored in the crawldb record of the url to the url's fetched content object. You need to specify the entry in the <b>scoring.db.md</b> property</li>
 *   <li>Content to parsedData: Copy a metadata entry stored in the Content object of a crawled url to its parsedData.  You need to specify the entry in the <b>scoring.content.md</b> property</li>
 *   <li>ParsedData to outlink objects: Copy a metadata entry stored in the parsedData of a crawl item to the crawldb records of the url's outlinks. You need to specify the entry in the <b>scoring.parse.md</b> property</li>
 * </ul>
 * <p>Note that you can not move data directly from a crawldb record to 
 * parseData or outlink objects. The sequence of moving the metadata 
 * should be crawldb -&gt; content -&gt; parsedData -&gt; outlink objects.</p>
 */
package org.apache.nutch.scoring.metadata;

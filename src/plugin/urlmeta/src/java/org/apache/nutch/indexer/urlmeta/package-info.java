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
 * <p>URL Meta Tag Indexing Plugin</p>
 * <p>Takes Meta Tags, injected alongside a URL 
 * (see <A href="https://issues.apache.org/jira/browse/NUTCH-655">NUTCH-655</a>) 
 * and specified in the "urlmeta.tags" property, and inserts them into 
 * the document--which is then sent to the Indexer. If you specify 
 * these fields in the Nutch schema (as well as the Indexer's), you 
 * can reasonably assume that they will be indexed.</p>
 */
package org.apache.nutch.indexer.urlmeta;

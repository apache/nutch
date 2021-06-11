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
 * Index content, configure and run indexing and cleaning jobs to 
 * add, update, and delete documents from an index. Two tasks are 
 * delegated to plugins:
 * <ul>
 *  <li>indexing filters, which fill index fields of each document</li>
 *  <li>index writer plugins; which send documents to index back-ends (Solr, etc.).</li>
 * </ul>
 */
package org.apache.nutch.indexer;

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

val luceneVersion = "8.11.2"

dependencies {
    implementation("org.elasticsearch.client:elasticsearch-rest-high-level-client:7.10.2")
    implementation("org.apache.lucene:lucene-analyzers-common:$luceneVersion")
    implementation("org.apache.lucene:lucene-backward-codecs:$luceneVersion")
    implementation("org.apache.lucene:lucene-core:$luceneVersion")
    implementation("org.apache.lucene:lucene-grouping:$luceneVersion")
    implementation("org.apache.lucene:lucene-highlighter:$luceneVersion")
    implementation("org.apache.lucene:lucene-join:$luceneVersion")
    implementation("org.apache.lucene:lucene-memory:$luceneVersion")
    implementation("org.apache.lucene:lucene-misc:$luceneVersion")
    implementation("org.apache.lucene:lucene-queries:$luceneVersion")
    implementation("org.apache.lucene:lucene-queryparser:$luceneVersion")
    implementation("org.apache.lucene:lucene-sandbox:$luceneVersion")
    implementation("org.apache.lucene:lucene-spatial-extras:$luceneVersion")
    implementation("org.apache.lucene:lucene-spatial3d:$luceneVersion")
    implementation("org.apache.lucene:lucene-suggest:$luceneVersion")
}


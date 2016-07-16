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

package org.apache.nutch.indexwriter.cloudsearch;

public interface CloudSearchConstants {
  public static final String CLOUDSEARCH_PREFIX = "cloudsearch.";
  public static final String ENDPOINT = CLOUDSEARCH_PREFIX + "endpoint";
  public static final String REGION = CLOUDSEARCH_PREFIX + "region";
  public static final String BATCH_DUMP = CLOUDSEARCH_PREFIX + "batch.dump";
  public static final String MAX_DOCS_BATCH = CLOUDSEARCH_PREFIX
      + "batch.maxSize";
}

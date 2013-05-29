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
package org.apache.nutch.indexer.solr;

public interface SolrConstants {
  public static final String SOLR_PREFIX = "solr.";

  public static final String SERVER_URL = SOLR_PREFIX + "server.url";

  public static final String COMMIT_SIZE = SOLR_PREFIX + "commit.size";
  
  public static final String COMMIT_INDEX = SOLR_PREFIX + "commit.index";

  public static final String MAPPING_FILE = SOLR_PREFIX + "mapping.file";

  public static final String USE_AUTH = SOLR_PREFIX + "auth";

  public static final String USERNAME = SOLR_PREFIX + "auth.username";

  public static final String PASSWORD = SOLR_PREFIX + "auth.password";
  
  public static final String ID_FIELD = "id";
  
  public static final String URL_FIELD = "url";
  
  public static final String BOOST_FIELD = "boost";
  
  public static final String TIMESTAMP_FIELD = "tstamp";
  
  public static final String DIGEST_FIELD = "digest";

}

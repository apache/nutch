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
package org.apache.nutch.indexwriter.hbase;

interface HBaseConstants {
  static final String HBASE_PREFIX = "hbase.indexer.";

  static final String HBASE_MAPPING_FILE = HBASE_PREFIX + "mapping.file";
  static final String HBASE_COMMIT_SIZE = HBASE_PREFIX + "commit.size";
  static final String ZOOKEEPER_QUORUM = HBASE_PREFIX + "zookeeper.quorum";
  static final String ZOOKEEPER_CLIENT_PORT = HBASE_PREFIX
      + "zookeeper.property.clientPort";
  static final String COMMIT_SIZE = HBASE_PREFIX + "commit.size";

  static final String DEFAULT_MAPPING_FILE = "hbaseindex-mapping.xml";
  static final String DEFAULT_ZOOKEEPER_QUORUM = "localhost";
  static final int DEFAULT_ZOOKEEPER_CLIENT_PORT = 2181;
  static final int DEFAULT_COMMIT_SIZE = 250;

  // Mapping file tags, attributes
  static final String TAG_FIELD = "field";
  static final String TAG_ROW = "row";
  static final String TAG_TABLE = "table";

  static final String ATTR_NAME = "name";
  static final String ATTR_DEST = "dest";
  static final String ATTR_SRC = "source";
  static final String ATTR_FAMILY = "family";
  static final String ATTR_QUALIFIER = "qualifier";

  static final String DEFAULT_TABLE_NAME = "document";
  static final String DEFAULT_COLUMN_FAMILY = "f";
  static final String DEFAULT_ROW_KEY = "id";
}

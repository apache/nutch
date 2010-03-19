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
package org.apache.nutch.indexer.lucene;

public interface LuceneConstants {
  public static final String LUCENE_PREFIX = "lucene.";

  public static final String FIELD_PREFIX = LUCENE_PREFIX + "field.";

  public static final String FIELD_STORE_PREFIX = FIELD_PREFIX + "store.";

  public static final String FIELD_INDEX_PREFIX = FIELD_PREFIX + "index.";

  public static final String FIELD_VECTOR_PREFIX = FIELD_PREFIX + "vector.";

  public static final String STORE_YES = "store.yes";

  public static final String STORE_NO = "store.no";

  public static final String STORE_COMPRESS = "store.compress";

  public static final String INDEX_NO = "index.no";

  // TODO: -> ANALYZED_NO_NORMS
  public static final String INDEX_NO_NORMS = "index.no_norms";

  // TODO: -> ANALYZED
  public static final String INDEX_TOKENIZED = "index.tokenized";

  // TODO: -> NOT_ANALYZED
  public static final String INDEX_UNTOKENIZED = "index.untokenized";

  public static final String VECTOR_NO = "vector.no";

  public static final String VECTOR_POS = "vector.pos";

  public static final String VECTOR_OFFSET = "vector.offset";

  public static final String VECTOR_POS_OFFSET = "vector.pos_offset";

  public static final String VECTOR_YES = "vector.yes";

}

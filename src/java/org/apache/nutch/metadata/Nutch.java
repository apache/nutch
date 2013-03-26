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
package org.apache.nutch.metadata;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.Text;


/**
 * A collection of Nutch internal metadata constants.
 *
 * @author Chris Mattmann
 * @author J&eacute;r&ocirc;me Charron
 */
public interface Nutch {

  public static final String ORIGINAL_CHAR_ENCODING =
          "OriginalCharEncoding";

  public static final String CHAR_ENCODING_FOR_CONVERSION =
          "CharEncodingForConversion";

  public static final String SIGNATURE_KEY = "nutch.content.digest";

  public static final String BATCH_NAME_KEY = "nutch.batch.name";

  public static final String SCORE_KEY = "nutch.crawl.score";

  public static final String GENERATE_TIME_KEY = "_ngt_";

  public static final Text WRITABLE_GENERATE_TIME_KEY = new Text(GENERATE_TIME_KEY);

  public static final String PROTO_STATUS_KEY = "_pst_";

  public static final Text WRITABLE_PROTO_STATUS_KEY = new Text(PROTO_STATUS_KEY);

  public static final String FETCH_TIME_KEY = "_ftk_";

  public static final String FETCH_STATUS_KEY = "_fst_";

  /** Sites may request that search engines don't provide access to cached documents. */
  public static final String CACHING_FORBIDDEN_KEY = "caching.forbidden";

  public static final Utf8 CACHING_FORBIDDEN_KEY_UTF8 = new Utf8(CACHING_FORBIDDEN_KEY);

  /** Show both original forbidden content and summaries (default). */
  public static final String CACHING_FORBIDDEN_NONE = "none";

  /** Don't show either original forbidden content or summaries. */
  public static final String CACHING_FORBIDDEN_ALL = "all";

  /** Don't show original forbidden content, but show summaries. */
  public static final String CACHING_FORBIDDEN_CONTENT = "content";

  public static final String REPR_URL_KEY = "_repr_";

  public static final Text WRITABLE_REPR_URL_KEY = new Text(REPR_URL_KEY);

  public static final String ALL_BATCH_ID_STR = "-all";

  public static final Utf8 ALL_CRAWL_ID = new Utf8(ALL_BATCH_ID_STR);

  public static final String CRAWL_ID_KEY = "storage.crawl.id";
  
  
  // short constants for cmd-line args
  /** Batch id to select. */
  public static final String ARG_BATCH = "batch";
  /** Crawl id to use. */
  public static final String ARG_CRAWL = "crawl";
  /** Resume previously aborted op. */
  public static final String ARG_RESUME = "resume";
  /** Force processing even if there are locks or inconsistencies. */
  public static final String ARG_FORCE = "force";
  /** Sort statistics. */
  public static final String ARG_SORT = "sort";
  /** Solr URL. */
  public static final String ARG_SOLR = "solr";
  /** Number of fetcher threads (per map task). */
  public static final String ARG_THREADS = "threads";
  /** Number of fetcher tasks. */
  public static final String ARG_NUMTASKS = "numTasks";
  /** Generate topN scoring URLs. */
  public static final String ARG_TOPN = "topN";
  /** The notion of current time. */
  public static final String ARG_CURTIME = "curTime";
  /** Apply URLFilters. */
  public static final String ARG_FILTER = "filter";
  /** Apply URLNormalizers. */
  public static final String ARG_NORMALIZE = "normalize";
  /** Whitespace-separated list of seed URLs. */
  public static final String ARG_SEEDLIST = "seed";
  /** a path to a directory containing a list of seed URLs. */
  public static final String ARG_SEEDDIR = "seedDir";
  /** Class to run as a NutchTool. */
  public static final String ARG_CLASS = "class";
  /** Depth (number of cycles) of a crawl. */
  public static final String ARG_DEPTH = "depth";
  
  // short constants for status / results fields
  /** Status / result message. */
  public static final String STAT_MESSAGE = "msg";
  /** Phase of processing. */
  public static final String STAT_PHASE = "phase";
  /** Progress (float). */
  public static final String STAT_PROGRESS = "progress";
  /** Jobs. */
  public static final String STAT_JOBS = "jobs";
  /** Counters. */
  public static final String STAT_COUNTERS = "counters";
}

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
package org.apache.nutch.indexer.field;

public interface Fields {

  // names of common fields
  public static final String ANCHOR = "anchor";
  public static final String SEGMENT = "segment";
  public static final String DIGEST = "digest";
  public static final String HOST = "host";
  public static final String SITE = "site";
  public static final String URL = "url";
  public static final String ORIG_URL = "orig";
  public static final String SEG_URL = "segurl";
  public static final String CONTENT = "content";
  public static final String TITLE = "title";
  public static final String CACHE = "cache";
  public static final String TSTAMP = "tstamp";
  public static final String BOOSTFACTOR = "boostfactor";
  
  // special fields for indexer
  public static final String BOOST = "boost";
  public static final String COMPUTATION = "computation";
  public static final String ACTION = "action";
}

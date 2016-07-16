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

import org.apache.hadoop.io.Text;

/**
 * A collection of Nutch internal metadata constants.
 * 
 * @author Chris Mattmann
 * @author J&eacute;r&ocirc;me Charron
 */
public interface Nutch {

	public static final String ORIGINAL_CHAR_ENCODING = "OriginalCharEncoding";

	public static final String CHAR_ENCODING_FOR_CONVERSION = "CharEncodingForConversion";

	public static final String SIGNATURE_KEY = "nutch.content.digest";

	public static final String SEGMENT_NAME_KEY = "nutch.segment.name";

	public static final String SCORE_KEY = "nutch.crawl.score";

	public static final String GENERATE_TIME_KEY = "_ngt_";

	public static final Text WRITABLE_GENERATE_TIME_KEY = new Text(
			GENERATE_TIME_KEY);

	public static final Text PROTOCOL_STATUS_CODE_KEY = new Text("nutch.protocol.code");

	public static final String PROTO_STATUS_KEY = "_pst_";

	public static final Text WRITABLE_PROTO_STATUS_KEY = new Text(
			PROTO_STATUS_KEY);

	public static final String FETCH_TIME_KEY = "_ftk_";

	public static final String FETCH_STATUS_KEY = "_fst_";

	/**
	 * Sites may request that search engines don't provide access to cached
	 * documents.
	 */
	public static final String CACHING_FORBIDDEN_KEY = "caching.forbidden";

	/** Show both original forbidden content and summaries (default). */
	public static final String CACHING_FORBIDDEN_NONE = "none";

	/** Don't show either original forbidden content or summaries. */
	public static final String CACHING_FORBIDDEN_ALL = "all";

	/** Don't show original forbidden content, but show summaries. */
	public static final String CACHING_FORBIDDEN_CONTENT = "content";

	public static final String REPR_URL_KEY = "_repr_";

	public static final Text WRITABLE_REPR_URL_KEY = new Text(REPR_URL_KEY);

	/** Used by AdaptiveFetchSchedule to maintain custom fetch interval */
	public static final String FIXED_INTERVAL_KEY = "fixedInterval";

	public static final Text WRITABLE_FIXED_INTERVAL_KEY = new Text(
			FIXED_INTERVAL_KEY);

	
	 /** For progress of job. Used by the Nutch REST service */
	public static final String STAT_PROGRESS = "progress";
	/**Used by Nutch REST service */
	public static final String CRAWL_ID_KEY = "storage.crawl.id";
	/** Argument key to specify location of the seed url dir for the REST endpoints **/
	public static final String ARG_SEEDDIR = "url_dir";
	/** Argument key to specify the location of crawldb for the REST endpoints **/
	public static final String ARG_CRAWLDB = "crawldb";
	/** Argument key to specify the location of linkdb for the REST endpoints **/
	public static final String ARG_LINKDB = "linkdb";
	/** Name of the key used in the Result Map sent back by the REST endpoint **/
	public static final String VAL_RESULT = "result";
	/** Argument key to specify the location of a directory of segments for the REST endpoints.
	 * Similar to the -dir command in the bin/nutch script **/
	public static final String ARG_SEGMENTDIR = "segment_dir";
	/** Argument key to specify the location of individual segment for the REST endpoints **/
	public static final String ARG_SEGMENT = "segment";
}

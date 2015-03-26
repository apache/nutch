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

package org.apache.nutch.tools;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * This class provides methods to map crawled data on JSON using Jettinson APIs. 
 *
 */
public class CommonCrawlFormatJettinson extends AbstractCommonCrawlFormat {
	
	private Deque<JSONObject> stack;

	public CommonCrawlFormatJettinson(String url, byte[] content,
			Metadata metadata, Configuration conf, String keyPrefix) throws IOException {
		super(url, content, metadata, conf, keyPrefix);
		
		stack = new ArrayDeque<JSONObject>();
	}
	
	@Override
	protected void writeKeyValue(String key, String value) throws IOException {
		try {
			stack.getFirst().put(key, value);
		} catch (JSONException jsone) {
			throw new IOException(jsone.getMessage());
		}
	}
	
	@Override
	protected void writeKeyNull(String key) throws IOException {
		try {
			stack.getFirst().put(key, JSONObject.NULL);
		} catch (JSONException jsone) {
			throw new IOException(jsone.getMessage());
		}
	}
	
	@Override
	protected void startObject(String key) throws IOException {
		JSONObject object = new JSONObject();
		stack.push(object);
	}
	
	@Override
	protected void closeObject(String key) throws IOException {
		try {
			if (stack.size() > 1) {
				JSONObject object = stack.pop();
				stack.getFirst().put(key, object);
			}
		} catch (JSONException jsone) {
			throw new IOException(jsone.getMessage());
		}
	}
	
	@Override
	protected String generateJson() throws IOException {
		try {
			return stack.getFirst().toString(2);
		} catch (JSONException jsone) {
			throw new IOException(jsone.getMessage());
		}
	}
}

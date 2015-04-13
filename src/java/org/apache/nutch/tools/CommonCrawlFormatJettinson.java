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
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * This class provides methods to map crawled data on JSON using Jettinson APIs. 
 *
 */
public class CommonCrawlFormatJettinson extends AbstractCommonCrawlFormat {
	
	private Deque<JSONObject> stackObjects;
	
	private Deque<JSONArray> stackArrays;

	public CommonCrawlFormatJettinson(String url, byte[] content, Metadata metadata, Configuration nutchConf, CommonCrawlConfig config) throws IOException {
		super(url, content, metadata, nutchConf, config);
		
		stackObjects = new ArrayDeque<JSONObject>();
		stackArrays = new ArrayDeque<JSONArray>();
	}
	
	@Override
	protected void writeKeyValue(String key, String value) throws IOException {
		try {
			stackObjects.getFirst().put(key, value);
		} catch (JSONException jsone) {
			throw new IOException(jsone.getMessage());
		}
	}
	
	@Override
	protected void writeKeyNull(String key) throws IOException {
		try {
			stackObjects.getFirst().put(key, JSONObject.NULL);
		} catch (JSONException jsone) {
			throw new IOException(jsone.getMessage());
		}
	}
	
	@Override
	protected void startArray(String key, boolean nested, boolean newline) throws IOException {
		JSONArray array = new JSONArray();
		stackArrays.push(array);
	}
	
	@Override
	protected void closeArray(String key, boolean nested, boolean newline) throws IOException {
		try {
			if (stackArrays.size() > 1) {
				JSONArray array = stackArrays.pop();
				if (nested) {
					stackArrays.getFirst().put(array);
				}
				else {
					stackObjects.getFirst().put(key, array);
				}
			}
		} catch (JSONException jsone) {
			throw new IOException(jsone.getMessage());
		}
	}
	
	@Override
	protected void writeArrayValue(String value) throws IOException {
		if (stackArrays.size() > 1) {
			stackArrays.getFirst().put(value);
		}
	}
	
	@Override
	protected void startObject(String key) throws IOException {
		JSONObject object = new JSONObject();
		stackObjects.push(object);
	}
	
	@Override
	protected void closeObject(String key) throws IOException {
		try {
			if (stackObjects.size() > 1) {
				JSONObject object = stackObjects.pop();
				stackObjects.getFirst().put(key, object);
			}
		} catch (JSONException jsone) {
			throw new IOException(jsone.getMessage());
		}
	}
	
	@Override
	protected String generateJson() throws IOException {
		try {
			return stackObjects.getFirst().toString(2);
		} catch (JSONException jsone) {
			throw new IOException(jsone.getMessage());
		}
	}
}

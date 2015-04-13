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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

/**
 * This class provides methods to map crawled data on JSON using Jackson Streaming APIs. 
 *
 */
public class CommonCrawlFormatJackson extends AbstractCommonCrawlFormat {
	
	private ByteArrayOutputStream out;
	
	private JsonGenerator generator;

	
	public CommonCrawlFormatJackson(String url, byte[] content, Metadata metadata, Configuration nutchConf, CommonCrawlConfig config) throws IOException {
		super(url, content, metadata, nutchConf, config);
		
		JsonFactory factory = new JsonFactory();
		this.out = new ByteArrayOutputStream();
		this.generator = factory.createGenerator(out);
		
		this.generator.useDefaultPrettyPrinter(); // INDENTED OUTPUT
	}
	
	@Override
	protected void writeKeyValue(String key, String value) throws IOException {
		generator.writeFieldName(key);
		generator.writeString(value);
	}
	
	@Override
	protected void writeKeyNull(String key) throws IOException {
		generator.writeFieldName(key);
		generator.writeNull();
	}
	
	@Override
	protected void startArray(String key, boolean nested, boolean newline) throws IOException {
		if (key != null) {
			generator.writeFieldName(key);
		}
		generator.writeStartArray();
	}
	
	@Override
	protected void closeArray(String key, boolean nested, boolean newline) throws IOException {
		generator.writeEndArray();
	}
	
	@Override
	protected void writeArrayValue(String value) throws IOException {
		generator.writeString(value);
	}
	
	@Override
	protected void startObject(String key) throws IOException {
		if (key != null) {
			generator.writeFieldName(key);
		}
		generator.writeStartObject();
	}
	
	@Override
	protected void closeObject(String key) throws IOException {
		generator.writeEndObject();
	}
	
	@Override
	protected String generateJson() throws IOException {
		this.generator.flush();
		return this.out.toString();
	}
}

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
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public class CommonCrawlConfig implements Serializable {

	/**
	 * Serial version UID
	 */
	private static final long serialVersionUID = 5235013733207799661L;
	
	// Prefix for key value in the output format
	private String keyPrefix = "";
	
	private boolean simpleDateFormat = false;
	
	private boolean jsonArray = false;
	
	private boolean reverseKey = false;
	
	private String reverseKeyValue = "";

	private boolean compressed = false;

	private long warcSize = 0;

	private String outputDir;
	
	/**
	 * Default constructor
	 */
	public CommonCrawlConfig() {
		// TODO init(this.getClass().getResourceAsStream("CommonCrawlConfig.properties"));
	}
	
	public CommonCrawlConfig(InputStream stream) {
		init(stream);
	}
	
	private void init(InputStream stream) {
		if (stream == null) {
			return;
		}
		Properties properties = new Properties();
		
		try {
			properties.load(stream);
		} catch (IOException e) {
			// TODO
		} finally {
			try {
				stream.close();
			} catch (IOException e) {
				// TODO
			}
		}

		setKeyPrefix(properties.getProperty("keyPrefix", ""));
		setSimpleDateFormat(Boolean.parseBoolean(properties.getProperty("simpleDateFormat", "False")));
		setJsonArray(Boolean.parseBoolean(properties.getProperty("jsonArray", "False")));
		setReverseKey(Boolean.parseBoolean(properties.getProperty("reverseKey", "False")));
	}
	
	public void setKeyPrefix(String keyPrefix) {
		this.keyPrefix = keyPrefix;
	}
	
	public void setSimpleDateFormat(boolean simpleDateFormat) {
		this.simpleDateFormat = simpleDateFormat;
	}
	
	public void setJsonArray(boolean jsonArray) {
		this.jsonArray = jsonArray;
	}
	
	public void setReverseKey(boolean reverseKey) {
		this.reverseKey = reverseKey;
	}
	
	public void setReverseKeyValue(String reverseKeyValue) {
		this.reverseKeyValue = reverseKeyValue;
	}
	
	public String getKeyPrefix() {
		return this.keyPrefix;
	}
	
	public boolean getSimpleDateFormat() {
		return this.simpleDateFormat;
	}
	
	public boolean getJsonArray() {
		return this.jsonArray;
	}
	
	public boolean getReverseKey() {
		return this.reverseKey;
	}
	
	public String getReverseKeyValue() {
		return this.reverseKeyValue;
	}

	public boolean isCompressed() {
		return compressed;
	}

	public void setCompressed(boolean compressed) {
		this.compressed = compressed;
	}

	public long getWarcSize() {
		return warcSize;
	}

	public void setWarcSize(long warcSize) {
		this.warcSize = warcSize;
	}

	public String getOutputDir() {
		return outputDir;
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}
}

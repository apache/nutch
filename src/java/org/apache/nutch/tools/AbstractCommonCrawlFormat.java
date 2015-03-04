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
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;

/**
 * Abstract class that implements {@see CommonCrawlFormat} interface. 
 *
 */
public abstract class AbstractCommonCrawlFormat implements CommonCrawlFormat {
	protected String url;
	
	protected byte[] content;
	
	protected Metadata metadata;
	
	protected Configuration conf;
	
	public AbstractCommonCrawlFormat(String url, byte[] content, Metadata metadata, Configuration conf) {
		this.url = url;
		this.content = content;
		this.metadata = metadata;
		this.conf = conf;
	}

	@Override
	public String getJsonData(boolean mapAll) throws IOException {
		if (mapAll) {
			return getJsonDataAll();
		}
		else {
			return getJsonDataSet();
		}
	}
	
	protected abstract String getJsonDataSet() throws IOException;
	
	protected abstract String getJsonDataAll() throws IOException;
	
	protected String ifNullString(String value) {
		return (value != null) ? value : "";
	}
	
	protected static String getHostName() {
		String hostName = "";
		try {
			hostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException uhe) {
			
		}
		return hostName;
	}
	
	protected static String getHostAddress() {
		String hostAddress = "";
		try {
			hostAddress = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException uhe) {
			
		}
		return hostAddress;
	}
}

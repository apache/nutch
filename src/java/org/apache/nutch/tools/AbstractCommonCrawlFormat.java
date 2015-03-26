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
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class that implements {@see CommonCrawlFormat} interface. 
 *
 */
public abstract class AbstractCommonCrawlFormat implements CommonCrawlFormat {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractCommonCrawlFormat.class.getName());
	
	protected String url;
	
	protected byte[] content;
	
	protected Metadata metadata;
	
	protected Configuration conf;
	
	protected String keyPrefix;
	
	public AbstractCommonCrawlFormat(String url, byte[] content, Metadata metadata, Configuration conf, String keyPrefix) throws IOException {
		this.url = url;
		this.content = content;
		this.metadata = metadata;
		this.conf = conf;
		this.keyPrefix = keyPrefix;
	}
	
	@Override
	public String getJsonData() throws IOException {
		try {
			startObject(null);
			
			// url
			writeKeyValue("url", getUrl());
			
			// timestamp
			writeKeyValue("timestamp", getTimestamp());
			
			// request
			startObject("request");
			writeKeyValue("method", getMethod());
			startObject("client");
			writeKeyValue("hostname", getRequestHostName());
			writeKeyValue("address", getRequestHostAddress());
			writeKeyValue("software", getRequestSoftware());
			writeKeyValue("robots", getRequestRobots());
			startObject("contact");
			writeKeyValue("name", getRequestContactName());
			writeKeyValue("email", getRequestContactEmail());
			closeObject("contact");
			closeObject("client");
			startObject("headers");
			writeKeyValue("Accept", getRequestAccept());
			writeKeyValue("Accept-Encoding", getRequestAcceptEncoding());
			writeKeyValue("Accept-Language", getRequestAcceptLanguage());
			writeKeyValue("User-Agent", getRequestUserAgent());
			closeObject("headers");
			writeKeyNull("body");
			closeObject("request");
			
			// response
			startObject("response");
			writeKeyValue("status", getResponseStatus());
			startObject("server");
			writeKeyValue("hostname", getResponseHostName());
			writeKeyValue("address", getResponseAddress());
			closeObject("server");
			startObject("headers");
			writeKeyValue("Content-Encoding", getResponseContentEncoding());
			writeKeyValue("Content-Type", getResponseContentType());
			writeKeyValue("Date", getResponseDate());
			writeKeyValue("Server", getResponseServer());
			for (String name : metadata.names()) {
				if (name.equalsIgnoreCase("Content-Encoding") || name.equalsIgnoreCase("Content-Type") || name.equalsIgnoreCase("Date") || name.equalsIgnoreCase("Server")) {
					continue;
				}
				writeKeyValue(name, metadata.get(name));
			}
			closeObject("headers");
			writeKeyValue("body", getResponseContent());
			closeObject("response");
			
			// key
			if (!this.keyPrefix.isEmpty()) {
				this.keyPrefix += "-";
			}
			writeKeyValue("key", this.keyPrefix + getKey());
			
			// imported
			writeKeyValue("imported", getImported());
			
			closeObject(null);
			
			return generateJson();
		
		} catch (IOException ioe) {
			LOG.warn("Error in processing file " + url + ": " + ioe.getMessage());
			throw new IOException("Error in generating JSON:" + ioe.getMessage()); 
		}
	}
	
	// abstract methods
	
	protected abstract void writeKeyValue(String key, String value) throws IOException;
	
	protected abstract void writeKeyNull(String key) throws IOException;
	
	protected abstract void startObject(String key) throws IOException;
	
	protected abstract void closeObject(String key) throws IOException;
	
	protected abstract String generateJson() throws IOException;
	
	// getters
	
	protected String getUrl() {
		return url;
	}
	
	protected String getTimestamp() {
		return metadata.get(ifNullString(Metadata.LAST_MODIFIED));
	}
	
	protected String getMethod() {
		return new String("GET");
	}
	
	protected String getRequestHostName() {
		String hostName = "";
		try {
			hostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException uhe) {
			
		}
		return hostName;
	}
	
	protected String getRequestHostAddress() {
		String hostAddress = "";
		try {
			hostAddress = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException uhe) {
			
		}
		return hostAddress;
	}
	
	protected String getRequestSoftware() {
		return conf.get("http.agent.version", "");
	}
	
	protected String getRequestRobots() {
		return new String("CLASSIC");
	}
	
	protected String getRequestContactName() {
		return conf.get("http.agent.name", "");
	}
	
	protected String getRequestContactEmail() {
		return conf.get("http.agent.email", "");
	}
	
	protected String getRequestAccept() {
		return conf.get("http.accept", "");
	}
	
	protected String getRequestAcceptEncoding() {
		return new String(""); // TODO
	}
	
	protected String getRequestAcceptLanguage() {
		return conf.get("http.accept.language", "");
	}
	
	protected String getRequestUserAgent() {
		return conf.get("http.robots.agents", "");
	}
	
	protected String getResponseStatus() {
		return ifNullString(metadata.get("status"));
	}
	
	protected String getResponseHostName() {
		return URLUtil.getHost(url);
	}
	
	protected String getResponseAddress() {
		return ifNullString(metadata.get("_ip_"));
	}
	
	protected String getResponseContentEncoding() {
		return ifNullString(metadata.get("Content-Encoding"));
	}
	
	protected String getResponseContentType() {
		return ifNullString(metadata.get("Content-Type"));
	}
	
	protected String getResponseDate() {
		return ifNullString(metadata.get("Date"));
	}
	
	protected String getResponseServer() {
		return ifNullString(metadata.get("Server"));
	}
	
	protected String getResponseContent() {
		return new String(content);
	}
	
	protected String getKey() {
		return url;
	}
	
	protected String getImported() {
		return new String(""); // TODO
	}
	
	private static String ifNullString(String value) {
		return (value != null) ? value : "";
	}
}

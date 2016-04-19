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
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.List;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.icu.text.SimpleDateFormat;

/**
 * Abstract class that implements {@see CommonCrawlFormat} interface. 
 *
 */
public abstract class AbstractCommonCrawlFormat implements CommonCrawlFormat {
	protected static final Logger LOG = LoggerFactory.getLogger(AbstractCommonCrawlFormat.class.getName());

	protected String url;

	protected Content content;

	protected Metadata metadata;

	protected Configuration conf;

	protected String keyPrefix;

	protected boolean simpleDateFormat;

	protected boolean jsonArray;

	protected boolean reverseKey;

	protected String reverseKeyValue;

	protected List<String> inLinks;

	public AbstractCommonCrawlFormat(String url, Content content, Metadata metadata, Configuration nutchConf, CommonCrawlConfig config) throws IOException {
		this.url = url;
		this.content = content;
		this.metadata = metadata;
		this.conf = nutchConf;

		this.keyPrefix = config.getKeyPrefix();
		this.simpleDateFormat = config.getSimpleDateFormat();
		this.jsonArray = config.getJsonArray();
		this.reverseKey = config.getReverseKey();
		this.reverseKeyValue = config.getReverseKeyValue();
	}

	public String getJsonData(String url, Content content, Metadata metadata)
      throws IOException {
		this.url = url;
		this.content = content;
		this.metadata = metadata;

		return this.getJsonData();
	}

	public String getJsonData(String url, Content content, Metadata metadata,
			ParseData parseData) throws IOException {

    // override of this is required in the actual formats
		throw new NotImplementedException();
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
			// start request headers
			startHeaders("headers", false, true);
			writeKeyValueWrapper("Accept", getRequestAccept());
			writeKeyValueWrapper("Accept-Encoding", getRequestAcceptEncoding());
			writeKeyValueWrapper("Accept-Language", getRequestAcceptLanguage());
			writeKeyValueWrapper("User-Agent", getRequestUserAgent());
			//closeObject("headers");
			closeHeaders("headers", false, true);
			writeKeyNull("body");
			closeObject("request");

			// response
			startObject("response");
			writeKeyValue("status", getResponseStatus());
			startObject("server");
			writeKeyValue("hostname", getResponseHostName());
			writeKeyValue("address", getResponseAddress());
			closeObject("server");
			// start response headers
			startHeaders("headers", false, true);
			writeKeyValueWrapper("Content-Encoding", getResponseContentEncoding());
			writeKeyValueWrapper("Content-Type", getResponseContentType());
			writeKeyValueWrapper("Date", getResponseDate());
			writeKeyValueWrapper("Server", getResponseServer());
			for (String name : metadata.names()) {
				if (name.equalsIgnoreCase("Content-Encoding") || name.equalsIgnoreCase("Content-Type") || name.equalsIgnoreCase("Date") || name.equalsIgnoreCase("Server")) {
					continue;
				}
				writeKeyValueWrapper(name, metadata.get(name));
			}
			closeHeaders("headers", false, true);
			writeKeyValue("body", getResponseContent());
			closeObject("response");

			// key
			if (!this.keyPrefix.isEmpty()) {
				this.keyPrefix += "-";
			}
			writeKeyValue("key", this.keyPrefix + getKey());

			// imported
			writeKeyValue("imported", getImported());

			if (getInLinks() != null){
				startArray("inlinks", false, true);
				for (String link : getInLinks()) {
					writeArrayValue(link);
				}
				closeArray("inlinks", false, true);
			}
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

	protected abstract void startArray(String key, boolean nested, boolean newline) throws IOException;

	protected abstract void closeArray(String key, boolean nested, boolean newline) throws IOException;

	protected abstract void writeArrayValue(String value) throws IOException;

	protected abstract void startObject(String key) throws IOException;

	protected abstract void closeObject(String key) throws IOException;

	protected abstract String generateJson() throws IOException;

	// getters

	protected String getUrl() {
		try {
			return URIUtil.encodePath(url);
		} catch (URIException e) {
			LOG.error("Can't encode URL " + url);
		}

		return url;
	}

	protected String getTimestamp() {
		if (this.simpleDateFormat) {
			String timestamp = null;
			try {
				long epoch = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z").parse(ifNullString(metadata.get(Metadata.LAST_MODIFIED))).getTime();
				timestamp = String.valueOf(epoch);
			} catch (ParseException pe) {
				LOG.warn(pe.getMessage());
			}
			return timestamp;
		} else {
			return ifNullString(metadata.get(Metadata.LAST_MODIFIED));
		}
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

	public List<String> getInLinks() {
		return inLinks;
	}

	public void setInLinks(List<String> inLinks) {
		this.inLinks = inLinks;
	}

	protected String getResponseDate() {
		if (this.simpleDateFormat) {
			String timestamp = null;
			try {
				long epoch = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z").parse(ifNullString(metadata.get("Date"))).getTime();
				timestamp = String.valueOf(epoch);
			} catch (ParseException pe) {
				LOG.warn(pe.getMessage());
			}
			return timestamp;
		} else {
			return ifNullString(metadata.get("Date"));
		}
	}

	protected String getResponseServer() {
		return ifNullString(metadata.get("Server"));
	}

	protected String getResponseContent() {
		return new String(content.getContent());
	}

	protected String getKey() {
		if (this.reverseKey) {
			return this.reverseKeyValue;
		}
		else {
			return url;
		}
	}

	protected String getImported() {
		if (this.simpleDateFormat) {
			String timestamp = null;
			try {
				long epoch = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z").parse(ifNullString(metadata.get("Date"))).getTime();
				timestamp = String.valueOf(epoch);
			} catch (ParseException pe) {
				LOG.warn(pe.getMessage());
			}
			return timestamp;
		} else {
			return ifNullString(metadata.get("Date"));
		}
	}

	private static String ifNullString(String value) {
		return (value != null) ? value : "";
	}

	private void startHeaders(String key, boolean nested, boolean newline) throws IOException {
		if (this.jsonArray) {
			startArray(key, nested, newline);
		}
		else {
			startObject(key);
		}
	}

	private void closeHeaders(String key, boolean nested, boolean newline) throws IOException {
		if (this.jsonArray) {
			closeArray(key, nested, newline);
		}
		else {
			closeObject(key);
		}
	}

	private void writeKeyValueWrapper(String key, String value) throws IOException {
		if (this.jsonArray) {
			startArray(null, true, false);
			writeArrayValue(key);
			writeArrayValue(value);
			closeArray(null, true, false);
		}
		else {
			writeKeyValue(key, value);
		}
	}

	@Override
	public void close() {}
}

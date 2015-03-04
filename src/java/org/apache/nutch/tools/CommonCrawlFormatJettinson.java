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

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.util.URLUtil;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides methods to map crawled data on JSON using Jettinson APIs. 
 *
 */
public class CommonCrawlFormatJettinson extends AbstractCommonCrawlFormat {
	
	private static final Logger LOG = LoggerFactory.getLogger(CommonCrawlFormatJettinson.class.getName());

	public CommonCrawlFormatJettinson(String url, byte[] content,
			Metadata metadata, Configuration conf) {
		super(url, content, metadata, conf);
	}
	
	@Override
	protected String getJsonDataAll() throws IOException {
		JSONObject object = new JSONObject();

		try {
			// url
			object.put("url", url);

			// timestamp
			object.put("timestamp", metadata.get(Metadata.LAST_MODIFIED));

			// request
			JSONObject requestObject = new JSONObject();
			requestObject.put("method", "GET"); 
			JSONObject clientObject = new JSONObject();
			clientObject.put("hostname", getHostName());
			clientObject.put("address", getHostAddress());
			clientObject.put("software", conf.get("http.agent.version", ""));
			clientObject.put("robots", "CLASSIC");
			JSONObject contactObject = new JSONObject();
			contactObject.put("name", conf.get("http.agent.name", ""));
			contactObject.put("email", conf.get("http.agent.email", ""));
			clientObject.put("contact", contactObject);
			requestObject.put("client", clientObject);
			JSONObject reqHeadersObject = new JSONObject();
			reqHeadersObject.put("Accept", conf.get("http.accept", ""));
			reqHeadersObject.put("Accept-Encoding", ""); // TODO
			reqHeadersObject.put("Accept-Language",	conf.get("http.accept.language", ""));
			reqHeadersObject.put("User-Agent", conf.get("http.robots.agents", ""));
			requestObject.put("headers", reqHeadersObject);
			requestObject.put("body", JSONObject.NULL);
			object.put("request", requestObject);

			// response
			JSONObject responseObject = new JSONObject();
			responseObject.put("status", ifNullString(metadata.get("status")));
			JSONObject serverObject = new JSONObject();
			serverObject.put("hostname", URLUtil.getHost(url));
			serverObject.put("address", ifNullString(metadata.get("_ip_")));
			responseObject.put("client", serverObject);
			JSONObject respHeadersObject = new JSONObject();
			for (String name : metadata.names()) {
				respHeadersObject.put(name, ifNullString(metadata.get(name)));
			}
			responseObject.put("headers", respHeadersObject);
			responseObject.put("body", new String(content));
			object.put("response", responseObject);

			// key
			object.put("key", url); 

			// imported
			object.put("imported", ""); // TODO

			return object.toString(2); // INDENTED OUTPUT

		} catch (JSONException jsone) {
			LOG.warn("Error in processing file " + url + ": " + jsone.getMessage());
			throw new IOException("Error in generating JSON using Jettinson:" + jsone.getMessage()); 
		}
	}

	@Override
	protected String getJsonDataSet() throws IOException {
		JSONObject object = new JSONObject();

		try {
			// url
			object.put("url", url);

			// timestamp
			object.put("timestamp", metadata.get(Metadata.LAST_MODIFIED));

			// request
			JSONObject requestObject = new JSONObject();
			requestObject.put("method", "GET"); 
			JSONObject clientObject = new JSONObject();
			clientObject.put("hostname", getHostName());
			clientObject.put("address", getHostAddress());
			clientObject.put("software", conf.get("http.agent.version", ""));
			clientObject.put("robots", "CLASSIC"); 
			JSONObject contactObject = new JSONObject();
			contactObject.put("name", conf.get("http.agent.name", ""));
			contactObject.put("email", conf.get("http.agent.email", ""));
			clientObject.put("contact", contactObject);
			requestObject.put("client", clientObject);
			JSONObject reqHeadersObject = new JSONObject();
			reqHeadersObject.put("Accept", conf.get("http.accept", ""));
			reqHeadersObject.put("Accept-Encoding", ""); // TODO
			reqHeadersObject.put("Accept-Language",	conf.get("http.accept.language", ""));
			reqHeadersObject.put("User-Agent", conf.get("http.robots.agents", "")); 
			requestObject.put("headers", reqHeadersObject);
			requestObject.put("body", JSONObject.NULL);
			object.put("request", requestObject);

			// response
			JSONObject responseObject = new JSONObject();
			responseObject.put("status", ifNullString(metadata.get("status")));
			JSONObject serverObject = new JSONObject();
			serverObject.put("hostname", URLUtil.getHost(url)); 
			serverObject.put("address", ifNullString(metadata.get("_ip_")));
			responseObject.put("client", serverObject);
			JSONObject respHeadersObject = new JSONObject();
			respHeadersObject.put("Content-Encoding", ifNullString(metadata.get("Content-Encoding")));
			respHeadersObject.put("Content-Type", ifNullString(metadata.get("Content-Type")));
			respHeadersObject.put("Date", ifNullString(metadata.get("Date")));
			respHeadersObject.put("Server", ifNullString(metadata.get("Server")));
			responseObject.put("headers", respHeadersObject);
			responseObject.put("body", new String(content)); 
			object.put("response", responseObject);

			// key
			object.put("key", url);

			// imported
			object.put("imported", ""); // TODO

			return object.toString(2); // INDENTED OUTPUT

		} catch (JSONException jsone) {
			LOG.warn("Error in processing file " + url + ": " + jsone.getMessage());
			throw new IOException("Error in generating JSON using Jettinson:" + jsone.getMessage()); 
		}
	}
}

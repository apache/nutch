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

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.util.URLUtil;

/**
 * This class provides methods to map crawled data on JSON using a {@see StringBuilder} object. 
 *
 */
public class CommonCrawlFormatSimple extends AbstractCommonCrawlFormat {
	
	public CommonCrawlFormatSimple(String url, byte[] content, Metadata metadata,
			Configuration conf) {
		super(url, content, metadata, conf);
	}
	
	@Override
	protected String getJsonDataAll() {
		// TODO character escaping
		StringBuilder sb = new StringBuilder();
		sb.append("{\n");

		// url
		sb.append("\t\"url\": \"" + url + "\",\n");
		
		// timstamp
		sb.append("\t\"timstamp\": \"" + metadata.get(Metadata.LAST_MODIFIED) + "\",\n");
				
		// request
		sb.append("\t\"request\": {\n");
		sb.append("\t\t\"method\": \"GET\",\n");
		sb.append("\t\t\"client\": {\n");
		sb.append("\t\t\t\"hostname\": \"" + getHostName() + "\",\n");
		sb.append("\t\t\t\"address\": \"" + getHostAddress() + "\",\n");
		sb.append("\t\t\t\"software\": \"" + conf.get("http.agent.version", "") + "\",\n");
		sb.append("\t\t\t\"robots\": \"CLASSIC\",\n");
		sb.append("\t\t\t\"contact\": {\n");
		sb.append("\t\t\t\t\"name\": \"" + conf.get("http.agent.name", "") + "\",\n");
		sb.append("\t\t\t\t\"email\": \"" + conf.get("http.agent.email", "") + "\",\n");
		sb.append("\t\t\t}\n");
		sb.append("\t\t},\n");
		sb.append("\t\t\"headers\": {\n");
		sb.append("\t\t\t\"Accept\": \"" + conf.get("http.accept", "") + "\",\n");
		sb.append("\t\t\t\"Accept-Encoding\": \"\",\n"); //TODO
		sb.append("\t\t\t\"Accept-Language\": \"" + conf.get("http.accept.language", "") + "\",\n");
		sb.append("\t\t\t\"User-Agent\": \"" + conf.get("http.robots.agents", "") + "\",\n");  
		sb.append("\t},\n");

		// response
		sb.append("\t\"response\": {\n");
		sb.append("\t\t\"status\": \"" + ifNullString(metadata.get("status")) + "\",\n");
		sb.append("\t\t\"server\": {\n");
		sb.append("\t\t\t\"hostname\": \"" + URLUtil.getHost(url) + "\"\n"); 
		sb.append("\t\t\t\"address\": \"" + metadata.get("_ip_") + "\"\n");
		sb.append("\t\t},\n");
		sb.append("\t\t\"headers\": {\n");	
		for (String name : metadata.names()) {
			sb.append("\t\t\t\"" + name + "\": \"" + metadata.get(name)	+ "\"\n");
		}
		sb.append("\t\t},\n");
		sb.append("\t\t\"body\": " + new String(content) + "\",\n");
		sb.append("\t},\n");
		
		// key
		sb.append("\t\"key\": \"" + url + "\",\n");
		
		// imported
		sb.append("\t\"imported\": \"\"\n"); //TODO
		
		sb.append("}");

		return sb.toString();
	}
	
	@Override
	protected String getJsonDataSet() {
		// TODO character escaping
		StringBuilder sb = new StringBuilder();
		sb.append("{\n");
		
		// url
		sb.append("\t\"url\": \"" + url + "\",\n");
		
		// timstamp
		sb.append("\t\"timestamp\": \"" + metadata.get(Metadata.LAST_MODIFIED) + "\",\n");
		
		// request
		sb.append("\t\"request\": {\n");
		sb.append("\t\t\"method\": \"GET\",\n");
		sb.append("\t\t\"client\": {\n");
		sb.append("\t\t\t\"hostname\": \"" + getHostName() + "\",\n");
		sb.append("\t\t\t\"address\": \"" + getHostAddress() + "\",\n");
		sb.append("\t\t\t\"software\": \"" + conf.get("http.agent.version", "") + "\",\n");
		sb.append("\t\t\t\"robots\": \"CLASSIC\",\n");
		sb.append("\t\t\t\"contact\": {\n");
		sb.append("\t\t\t\t\"name\": \"" + conf.get("http.agent.name", "") + "\",\n");
		sb.append("\t\t\t\t\"email\": \"" + conf.get("http.agent.email", "") + "\",\n");
		sb.append("\t\t\t}\n");
		sb.append("\t\t},\n");
		sb.append("\t\t\"headers\": {\n");
		sb.append("\t\t\t\"Accept\": \"" + conf.get("http.accept", "") + "\",\n");
		sb.append("\t\t\t\"Accept-Encoding\": \"\",\n"); // TODO
		sb.append("\t\t\t\"Accept-Language\": \"" + conf.get("http.accept.language", "") + "\",\n");
    sb.append("\t\t\t\"User-Agent\": \"" + conf.get("http.robots.agents", "") + "\",\n");  
		sb.append("\t},\n");
		
		// response
		sb.append("\t\"response\": {\n");
		sb.append("\t\t\"status\": \"" + ifNullString(metadata.get("status")) + "\",\n");
		sb.append("\t\t\"server\": {\n");
    sb.append("\t\t\t\"hostname\": \"" + URLUtil.getHost(url) + "\"\n"); 
		sb.append("\t\t\t\"address\": \"" + metadata.get("_ip_") + "\"\n");
		sb.append("\t\t},\n");
		sb.append("\t\t\"headers\": {\n");
		sb.append("\t\t\t\"Content-Encoding\": " + ifNullString(metadata.get("Content-Encoding")));
		sb.append("\t\t\t\"Content-Type\": " + ifNullString(metadata.get("Content-Type")));
		sb.append("\t\t\t\"Date\": " + ifNullString(metadata.get("Date")));
		sb.append("\t\t\t\"Server\": " + ifNullString(metadata.get("Server")));
		sb.append("\t\t},\n");
		sb.append("\t\t\"body\": " + new String(content) + "\",\n");
		sb.append("\t},\n");
		
		// key
		sb.append("\t\"key\": \"" + url + "\",\n"); 
		
		// imported
		sb.append("\t\"imported\": \"\"\n"); // TODO
		
		sb.append("}");

		return sb.toString();
	}

}

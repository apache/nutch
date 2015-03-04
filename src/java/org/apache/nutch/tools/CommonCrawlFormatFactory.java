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

/**
 * Factory class that creates new {@see CommonCrawlFormat} objects (a.k.a. formatter) that map crawled files to CommonCrawl format.   
 *
 */
public class CommonCrawlFormatFactory {
	
	/**
	 * Returns a new instance of a {@see CommonCrawlFormat} object specifying the type of formatter. 
	 * @param formatType the type of formatter to be created.
	 * @param url the url.
	 * @param content the content.
	 * @param metadata the metadata.
	 * @param conf the configuration.
	 * @return the new {@see CommonCrawlFormat} object.
	 */
	public static CommonCrawlFormat getCommonCrawlFormat(String formatType, String url, byte[] content,
			Metadata metadata, Configuration conf) {
		if (formatType == null) {
			return null;
		}
		
		if (formatType.equalsIgnoreCase("jackson")) {
			return new CommonCrawlFormatJackson(url, content, metadata, conf);
		}
		else if (formatType.equalsIgnoreCase("jettinson")) {
			return new CommonCrawlFormatJettinson(url, content, metadata, conf);
		}
		else if (formatType.equalsIgnoreCase("simple")) {
			return new CommonCrawlFormatSimple(url, content, metadata, conf);
		}
		
		return null;
	}
}

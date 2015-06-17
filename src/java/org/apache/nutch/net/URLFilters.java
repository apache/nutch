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

package org.apache.nutch.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.PluginRepository;

/** Creates and caches {@link URLFilter} implementing plugins. */
public class URLFilters {

	public static final String URLFILTER_ORDER = "urlfilter.order";
	private URLFilter[] filters;
	private URLFilter filter = null;

	public URLFilters(Configuration conf) {
		this.filters = (URLFilter[]) PluginRepository.get(conf)
				.getOrderedPlugins(URLFilter.class, URLFilter.X_POINT_ID,
						URLFILTER_ORDER);
	}

	/** Run all defined filters. Assume logical AND. */
	public String filter(String urlString) throws URLFilterException {
		for (int i = 0; i < this.filters.length; i++) {
			if (urlString == null)
				return null;
			urlString = this.filters[i].filter(urlString);

		}
		return urlString;
	}

	/**Get a filter with the full classname if only it is activated through the nutchsite.xml*/
	public URLFilter getFilter(String pid) {

		if (filter == null) {

			for (int i = 0; i < this.filters.length; i++) {

				if (filters[i].getClass().getName().equals(pid)) {

					filter = filters[i];
					break;
				}

			}

		}
		return filter;

	}
}

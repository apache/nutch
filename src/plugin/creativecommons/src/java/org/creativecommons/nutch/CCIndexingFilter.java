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

package org.creativecommons.nutch;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.CreativeCommons;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.Bytes;

/** Adds basic searchable fields to a document. */
public class CCIndexingFilter implements IndexingFilter {
	public static final Logger LOG = LoggerFactory.getLogger(CCIndexingFilter.class);

	/** The name of the document field we use. */
	public static String FIELD = "cc";

	private Configuration conf;

	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	static {
		FIELDS.add(WebPage.Field.BASE_URL);
		FIELDS.add(WebPage.Field.METADATA);
	}

	/**
	 * Add the features represented by a license URL. Urls are of the form
	 * "http://creativecommons.org/licenses/xx-xx/xx/xx", where "xx" names a
	 * license feature.
	 */
	public void addUrlFeatures(NutchDocument doc, String urlString) {
		try {
			URL url = new URL(urlString);

			// tokenize the path of the url, breaking at slashes and dashes
			StringTokenizer names = new StringTokenizer(url.getPath(), "/-");

			if (names.hasMoreTokens())
				names.nextToken(); // throw away "licenses"

			// add a feature per component after "licenses"
			while (names.hasMoreTokens()) {
				String feature = names.nextToken();
				addFeature(doc, feature);
			}
		} catch (MalformedURLException e) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("CC: failed to parse url: " + urlString + " : " + e);
			}
		}
	}

	private void addFeature(NutchDocument doc, String feature) {
		doc.add(FIELD, feature);
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public Collection<Field> getFields() {
		return FIELDS;
	}

	@Override
	public NutchDocument filter(NutchDocument doc, String url, WebPage page)
			throws IndexingException {

		ByteBuffer blicense = page.getFromMetadata(new Utf8(
				CreativeCommons.LICENSE_URL));
		if (blicense != null) {
			String licenseUrl = Bytes.toString(blicense);
			if (LOG.isInfoEnabled()) {
				LOG.info("CC: indexing " + licenseUrl + " for: "
						+ url.toString());
			}

			// add the entire license as cc:license=xxx
			addFeature(doc, "license=" + licenseUrl);

			// index license attributes extracted of the license url
			addUrlFeatures(doc, licenseUrl);
		}

		// index the license location as cc:meta=xxx
		ByteBuffer blicenseloc = page.getFromMetadata(new Utf8(
				CreativeCommons.LICENSE_LOCATION));
		if (blicenseloc != null) {
			String licenseLocation = Bytes.toString(blicenseloc);
			addFeature(doc, "meta=" + licenseLocation);
		}

		// index the work type cc:type=xxx
		ByteBuffer bworkType = page.getFromMetadata(new Utf8(
				CreativeCommons.WORK_TYPE));
		if (bworkType != null) {
			String workType = Bytes.toString(bworkType);
			addFeature(doc, workType);
		}

		return doc;
	}

}

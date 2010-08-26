/*
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
package org.apache.nutch.indexer.subcollection;

import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.collection.CollectionManager;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.NutchConfiguration;

public class SubcollectionIndexingFilter extends Configured implements
		IndexingFilter {

	public SubcollectionIndexingFilter() {
		super(NutchConfiguration.create());
	}

	public SubcollectionIndexingFilter(Configuration conf) {
		super(conf);
	}

	/**
	 * Doc field name
	 */
	public static final String FIELD_NAME = "subcollection";

	/**
	 * Logger
	 */
	public static final Logger LOG = LoggerFactory
			.getLogger(SubcollectionIndexingFilter.class);

	/**
	 * "Mark" document to be a part of subcollection
	 * 
	 * @param doc
	 * @param url
	 */
	private void addSubCollectionField(NutchDocument doc, String url) {
		for (String collname: CollectionManager.getCollectionManager(getConf()).getSubCollections(url)) {
			doc.add(FIELD_NAME, collname);
		}
	}

	@Override
	public Collection<Field> getFields() {
		return new ArrayList<Field>();
	}

	@Override
	public NutchDocument filter(NutchDocument doc, String url, WebPage page)
			throws IndexingException {
		addSubCollectionField(doc, url);
		return doc;
	}
}

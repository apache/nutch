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
package org.apache.nutch.indexer.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestJexlIndexingFilter {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testAllowMatchingDocument() throws Exception {
		Configuration conf = NutchConfiguration.create();
		conf.set("index.jexl.filter", "doc.lang[0]=='en'");

		JexlIndexingFilter filter = new JexlIndexingFilter();
		filter.setConf(conf);
		Assert.assertNotNull(filter);

		NutchDocument doc = new NutchDocument();

		String title = "The Foo Page";
		Outlink[] outlinks = new Outlink[] { new Outlink("http://foo.com/", "Foo") };
		Metadata metaData = new Metadata();
		metaData.add("Language", "en/us");
		ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, title, outlinks, metaData);
		ParseImpl parse = new ParseImpl("this is a sample foo bar page. hope you enjoy it.", parseData);

		CrawlDatum crawlDatum = new CrawlDatum();
		crawlDatum.setFetchTime(100L);

		Inlinks inlinks = new Inlinks();

		doc.add("lang", "en");

		NutchDocument result = filter.filter(doc, parse, new Text("http://nutch.apache.org/index.html"), crawlDatum, inlinks);
		Assert.assertNotNull(result);
		Assert.assertEquals(doc, result);
	}

	@Test
	public void testBlockNotMatchingDocuments() throws Exception {
		Configuration conf = NutchConfiguration.create();
		conf.set("index.jexl.filter", "doc.lang[0]=='en'");

		JexlIndexingFilter filter = new JexlIndexingFilter();
		filter.setConf(conf);
		Assert.assertNotNull(filter);

		NutchDocument doc = new NutchDocument();

		String title = "The Foo Page";
		Outlink[] outlinks = new Outlink[] { new Outlink("http://foo.com/", "Foo") };
		Metadata metaData = new Metadata();
		metaData.add("Language", "en/us");
		ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, title, outlinks, metaData);
		ParseImpl parse = new ParseImpl("this is a sample foo bar page. hope you enjoy it.", parseData);

		CrawlDatum crawlDatum = new CrawlDatum();
		crawlDatum.setFetchTime(100L);

		Inlinks inlinks = new Inlinks();

		doc.add("lang", "ru");
		
		NutchDocument result = filter.filter(doc, parse, new Text("http://nutch.apache.org/index.html"), crawlDatum, inlinks);
		Assert.assertNull(result);
	}

	@Test
	public void testMissingConfiguration() throws Exception {
		Configuration conf = NutchConfiguration.create();

		JexlIndexingFilter filter = new JexlIndexingFilter();
		thrown.expect(RuntimeException.class);
		filter.setConf(conf);
	}

	@Test
	public void testInvalidExpression() throws Exception {
		Configuration conf = NutchConfiguration.create();
		conf.set("index.jexl.filter", "doc.lang[0]=<>:='en'");

		JexlIndexingFilter filter = new JexlIndexingFilter();
		thrown.expect(RuntimeException.class);
		filter.setConf(conf);
	}
}

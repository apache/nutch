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

import java.lang.invoke.MethodHandles;
import java.util.Map.Entry;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.MapContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.util.JexlUtil;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An {@link org.apache.nutch.indexer.IndexingFilter} that allows filtering of
 * documents based on a JEXL expression.
 *
 */
public class JexlIndexingFilter implements IndexingFilter {

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private Configuration conf;
	private Expression expr;

	@Override
	public NutchDocument filter(NutchDocument doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks) throws IndexingException {
	    if (expr != null) {
	        // Create a context and add data
	        JexlContext jcontext = new MapContext();
	        
	        jcontext.set("status", CrawlDatum.getStatusName(datum.getStatus()));
	        jcontext.set("fetchTime", (long)(datum.getFetchTime()));
	        jcontext.set("modifiedTime", (long)(datum.getModifiedTime()));
	        jcontext.set("retries", datum.getRetriesSinceFetch());
	        jcontext.set("interval", new Integer(datum.getFetchInterval()));
	        jcontext.set("score", datum.getScore());
	        jcontext.set("signature", StringUtil.toHexString(datum.getSignature()));
	        jcontext.set("url", url.toString());
	        
	        jcontext.set("text", parse.getText());
	        jcontext.set("title", parse.getData().getTitle());

	        JexlContext httpStatusContext = new MapContext();
	        httpStatusContext.set("majorCode", parse.getData().getStatus().getMajorCode());
	        httpStatusContext.set("minorCode", parse.getData().getStatus().getMinorCode());
	        httpStatusContext.set("message", parse.getData().getStatus().getMessage());
	        jcontext.set("httpStatus", httpStatusContext);
	        
	        jcontext.set("documentMeta", metadataToContext(doc.getDocumentMeta()));
	        jcontext.set("contentMeta", metadataToContext(parse.getData().getContentMeta()));
	        jcontext.set("parseMeta", metadataToContext(parse.getData().getParseMeta()));
	        
	        JexlContext context = new MapContext();
	        for (Entry<String, NutchField> entry : doc) {
	        	context.set(entry.getKey(), entry.getValue().getValues());
	        }
	        jcontext.set("doc", context);
	                    
			try {
				if (Boolean.TRUE.equals(expr.evaluate(jcontext))) {
					return doc;
				}
			} catch (Exception e) {
				LOG.warn("Failed in evaluating JEXL {}", expr.getExpression(), e);
			}
	        return null;
	      }

		return doc;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		expr = JexlUtil.parseExpression(conf.get("index.jexl.filter"));
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	private JexlContext metadataToContext(Metadata metadata) {
		JexlContext context = new MapContext();
		for (String name : metadata.names()) {
			context.set(name, metadata.getValues(name));
		}
		return context;
	}
}

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
package org.apache.nutch.indexer.solr;

import java.io.IOException;
import java.net.MalformedURLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.IndexCleanerJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.ToolUtil;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;

public class SolrClean extends IndexCleanerJob {

	public static final int NUM_MAX_DELETE_REQUEST = 1000;
	public static final Logger LOG = LoggerFactory.getLogger(SolrClean.class);	

	public static class CleanReducer extends
			Reducer<String, WebPage, NullWritable, NullWritable> {
		private int numDeletes = 0;
		private SolrServer solr;
		private UpdateRequest updateRequest = new UpdateRequest();
		private boolean commit;

		@Override
		public void setup(Context job) throws IOException {
			Configuration conf = job.getConfiguration();
			commit = conf.getBoolean(ARG_COMMIT, true);
			try {
				solr = new CommonsHttpSolrServer(
						conf.get(SolrConstants.SERVER_URL));
			} catch (MalformedURLException e) {
				throw new IOException(e);
			}
		}

		public void reduce(String key, Iterable<WebPage> values, Context context)
				throws IOException {
	        updateRequest.deleteById(key);
			numDeletes++;
			if (numDeletes >= NUM_MAX_DELETE_REQUEST) {
				try {
					updateRequest.process(solr);
					context.getCounter("SolrClean", "DELETED").increment(
							numDeletes);

				} catch (SolrServerException e) {
					throw new IOException(e);
				}
				updateRequest = new UpdateRequest();
				numDeletes = 0;
			}
		}

		@Override
		public void cleanup(Context context) throws IOException {
			try {
				if (numDeletes > 0) {
					updateRequest.process(solr);
					context.getCounter("SolrClean", "DELETED").increment(
							numDeletes);
					if (commit) {
						solr.commit();
					}
				}
			} catch (SolrServerException e) {
				throw new IOException(e);
			}
		}
	}
	
	public Class<? extends Reducer<String, WebPage, NullWritable, NullWritable>> getReducerClass(){
		return CleanReducer.class;
	}	

	public int delete(String solrUrl, boolean commit) throws Exception {
		LOG.info("CleanJob: starting");
		run(ToolUtil.toArgMap(Nutch.ARG_SOLR, solrUrl, IndexCleanerJob.ARG_COMMIT, commit));
		LOG.info("CleanJob: done");
		return 0;
	}

	public int run(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage: SolrClean <solrurl> [-noCommit]");
			return 1;
		}

		boolean commit = true;
		if (args.length == 2 && args[1].equals("-noCommit")) {
			commit = false;
		}

		return delete(args[0], commit);
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(NutchConfiguration.create(),
				new SolrClean(), args);
		System.exit(result);
	}

}

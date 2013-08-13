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
package org.apache.nutch.indexer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.StringComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.solr.SolrConstants;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.ToolUtil;

public abstract class IndexCleanerJob extends NutchTool implements Tool {

	public static final String ARG_COMMIT = "commit";
	public static final Logger LOG = LoggerFactory.getLogger(IndexCleanerJob.class);
	private Configuration conf;

	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	static {
		FIELDS.add(WebPage.Field.STATUS);
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public static class CleanMapper extends
			GoraMapper<String, WebPage, String, WebPage> {
		
		  private IndexCleaningFilters filters;
		
		@Override
		  protected void setup(Context context) throws IOException {
		    Configuration conf = context.getConfiguration();
		    filters = new IndexCleaningFilters(conf);
		  }

		@Override
		public void map(String key, WebPage page, Context context)
				throws IOException, InterruptedException {
			try {				
				 if(page.getStatus() == CrawlStatus.STATUS_GONE || filters.remove(key, page)) {
					context.write(key, page);
				}
			} catch (IndexingException e) {
				LOG.warn("Error indexing "+key+": "+e);
			}
		}
	}

	public Collection<WebPage.Field> getFields(Job job) {
		Configuration conf = job.getConfiguration();
	    Collection<WebPage.Field> columns = new HashSet<WebPage.Field>(FIELDS);		
	    IndexCleaningFilters filters = new IndexCleaningFilters(conf);
	    columns.addAll(filters.getFields());
	    return columns;
	}	
	
	public abstract Class<? extends Reducer<String, WebPage, NullWritable, NullWritable>> getReducerClass();
	
	@Override
	public Map<String, Object> run(Map<String, Object> args) throws Exception {
		String solrUrl = (String) args.get(Nutch.ARG_SOLR);
		getConf().set(SolrConstants.SERVER_URL, solrUrl);
		getConf().setBoolean(ARG_COMMIT,(Boolean)args.get(ARG_COMMIT));
		currentJob = new NutchJob(getConf(), "index-clean");
		currentJob.getConfiguration().setClass(
				"mapred.output.key.comparator.class", StringComparator.class,
				RawComparator.class);

		Collection<WebPage.Field> fields = getFields(currentJob);
		StorageUtils.initMapperJob(currentJob, fields, String.class,
				WebPage.class, CleanMapper.class);
		currentJob.setReducerClass(getReducerClass());
		currentJob.setOutputFormatClass(NullOutputFormat.class);
		currentJob.waitForCompletion(true);
		ToolUtil.recordJobStatus(null, currentJob, results);
		return results;
	}	

}

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

package org.apache.nutch.util.domain;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.query.Query;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.WebTableReader.WebTableRegexMapper;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchJobConf;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.URLUtil;

/**
 * Extracts some very basic statistics about domains from the crawldb
 */
public class DomainStatistics extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory.getLogger(DomainStatistics.class);

	private static final Text FETCHED_TEXT = new Text("FETCHED");
	private static final Text NOT_FETCHED_TEXT = new Text("NOT_FETCHED");

	public static enum MyCounter {
		FETCHED, NOT_FETCHED, EMPTY_RESULT
	};

	private static final int MODE_HOST = 1;
	private static final int MODE_DOMAIN = 2;
	private static final int MODE_SUFFIX = 3;

	private Configuration conf;

	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
		if (args.length < 3) {
			System.out
					.println("usage: DomainStatistics outDir host|domain|suffix [numOfReducer]");
			return 1;
		}
		String outputDir = args[0];
		int numOfReducers = 1;

		if (args.length > 2) {
			numOfReducers = Integer.parseInt(args[2]);
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		LOG.info("DomainStatistics: starting at " + sdf.format(start));

		Job job = new NutchJob(getConf(), "Domain statistics");

		int mode = 0;
		if (args[1].equals("host"))
			mode = MODE_HOST;
		else if (args[1].equals("domain"))
			mode = MODE_DOMAIN;
		else if (args[1].equals("suffix"))
			mode = MODE_SUFFIX;
		job.getConfiguration().setInt("domain.statistics.mode", mode);

		DataStore<String, WebPage> store = StorageUtils.createWebStore(
				job.getConfiguration(), String.class, WebPage.class);

		Query<String, WebPage> query = store.newQuery();
		query.setFields(WebPage._ALL_FIELDS);

		GoraMapper.initMapperJob(job, query, store, Text.class, LongWritable.class,
				DomainStatisticsMapper.class, null, true);

		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setReducerClass(DomainStatisticsReducer.class);
		job.setCombinerClass(DomainStatisticsCombiner.class);
		job.setNumReduceTasks(numOfReducers);

		boolean success = job.waitForCompletion(true);

		long end = System.currentTimeMillis();
		LOG.info("DomainStatistics: finished at " + sdf.format(end)
				+ ", elapsed: " + TimingUtil.elapsedTime(start, end));

		if (!success)
			return -1;
		return 0;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public static class DomainStatisticsCombiner extends
			Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {

			long total = 0;

			for (LongWritable val : values)
				total += val.get();

			context.write(key, new LongWritable(total));
		}

	}

	public static class DomainStatisticsReducer extends
			Reducer<Text, LongWritable, LongWritable, Text> {

		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {

			long total = 0;

			for (LongWritable val : values)
				total += val.get();

			// invert output
			context.write(new LongWritable(total), key);
		}
	}

	public static class DomainStatisticsMapper extends
			GoraMapper<String, WebPage, Text, LongWritable> {
		LongWritable COUNT_1 = new LongWritable(1);

		private int mode = 0;

		public DomainStatisticsMapper() {
		}

		public void setup(Context context) {
			mode = context.getConfiguration().getInt("domain.statistics.mode",
					MODE_DOMAIN);
		}

		public void close() {
		}

		@Override
		protected void map(
				String key,
				WebPage value,
				org.apache.hadoop.mapreduce.Mapper<String, WebPage, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			if (value.getStatus() == CrawlStatus.STATUS_FETCHED) {
				try {
					URL url = new URL(key.toString());
					String out = null;
					switch (mode) {
					case MODE_HOST:
						out = url.getHost();
						break;
					case MODE_DOMAIN:
						out = URLUtil.getDomainName(url);
						break;
					case MODE_SUFFIX:
						out = URLUtil.getDomainSuffix(url).getDomain();
						break;
					}
					if (out.trim().equals("")) {
						LOG.info("url : " + url);
						context.getCounter(MyCounter.EMPTY_RESULT).increment(1);
					}

					context.write(new Text(out), COUNT_1);
				} catch (Exception ex) {
				}
				context.getCounter(MyCounter.FETCHED).increment(1);
				context.write(FETCHED_TEXT, COUNT_1);
			} else {
				context.getCounter(MyCounter.FETCHED).increment(1);
				context.write(NOT_FETCHED_TEXT, COUNT_1);
			}

		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(NutchConfiguration.create(), new DomainStatistics(),
				args);
	}

}

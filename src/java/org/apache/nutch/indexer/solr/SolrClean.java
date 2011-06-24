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
import java.text.SimpleDateFormat;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;

/**
* The class scans CrawlDB looking for entries with status DB_GONE (404) and sends delete requests to Solr
* for those documents.
* 
* 
* @author Claudio Martella
*
*/

public class SolrClean implements Tool {
  public static final Log LOG = LogFactory.getLog(SolrClean.class);
  private Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public static class DBFilter implements Mapper<Text, CrawlDatum, ByteWritable, Text> {
    private ByteWritable OUT = new ByteWritable(CrawlDatum.STATUS_DB_GONE);

    @Override
    public void configure(JobConf arg0) { }

    @Override
    public void close() throws IOException { }

    @Override
    public void map(Text key, CrawlDatum value,
        OutputCollector<ByteWritable, Text> output, Reporter reporter)
        throws IOException {

      if (value.getStatus() == CrawlDatum.STATUS_DB_GONE) {
        output.collect(OUT, key);
      }
    }
  }

  public static class SolrDeleter implements Reducer<ByteWritable, Text, Text, ByteWritable> {
    private static final int NUM_MAX_DELETE_REQUEST = 1000;
    private int numDeletes = 0;
    private int totalDeleted = 0;
    private SolrServer solr;
    private UpdateRequest updateRequest = new UpdateRequest();
    private boolean noCommit = false;

    @Override
    public void configure(JobConf job) {
      try {
        solr = new CommonsHttpSolrServer(job.get(SolrConstants.SERVER_URL));
        noCommit = job.getBoolean("noCommit", false);
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() throws IOException {
      try {
        if (numDeletes > 0) {
          LOG.info("SolrClean: deleting " + numDeletes + " documents");
          updateRequest.process(solr);
          totalDeleted += numDeletes;
        }

        if (totalDeleted > 0 && !noCommit) {
          solr.commit();
        }

        LOG.info("SolrClean: deleted a total of " + totalDeleted + " documents");
      } catch (SolrServerException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(ByteWritable key, Iterator<Text> values,
        OutputCollector<Text, ByteWritable> output, Reporter reporter)
    throws IOException {
      while (values.hasNext()) {
        Text document = values.next();
        updateRequest.deleteById(document.toString());
        numDeletes++;
        if (numDeletes >= NUM_MAX_DELETE_REQUEST) {
          try {
            LOG.info("SolrClean: deleting " + numDeletes + " documents");
            updateRequest.process(solr);
          } catch (SolrServerException e) {
            throw new IOException(e);
          }
          updateRequest = new UpdateRequest();
          totalDeleted += numDeletes;
          numDeletes = 0;
        }
      }
    }
  }

  public void delete(String crawldb, String solrUrl, boolean noCommit) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("SolrClean: starting at " + sdf.format(start));

    JobConf job = new NutchJob(getConf());

    FileInputFormat.addInputPath(job, new Path(crawldb, CrawlDb.CURRENT_NAME));
    job.setBoolean("noCommit", noCommit);
    job.set(SolrConstants.SERVER_URL, solrUrl);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setOutputFormat(NullOutputFormat.class);
    job.setMapOutputKeyClass(ByteWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapperClass(DBFilter.class);
    job.setReducerClass(SolrDeleter.class);

    JobClient.runJob(job);

    long end = System.currentTimeMillis();
    LOG.info("SolrClean: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
  }

  public int run(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println("Usage: SolrClean <crawldb> <solrurl> [-noCommit]");
      return 1;
    }

    boolean noCommit = false;
    if (args.length == 3 && args[2].equals("-noCommit")) {
      noCommit = true;
    }

    delete(args[0], args[1], noCommit);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(NutchConfiguration.create(),
        new SolrClean(), args);
    System.exit(result);
  }
}
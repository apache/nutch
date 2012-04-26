/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.host;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.gora.query.Query;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.Host;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scans the web table and create host entries for each unique host.
 * 
 * 
 **/

public class HostDbUpdateJob implements Tool {

  public static final Logger LOG = LoggerFactory
      .getLogger(HostDbUpdateJob.class);
  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  private Configuration conf;

  static {
    FIELDS.add(WebPage.Field.STATUS);
  }

  /**
   * Maps each WebPage to a host key.
   */
  public static class Mapper extends GoraMapper<String, WebPage, Text, WebPage> {

    @Override
    protected void map(String key, WebPage value, Context context)
        throws IOException, InterruptedException {

      String reversedHost = TableUtil.getReversedHost(key);
      context.write(new Text(reversedHost), value);
    }
  }

  public HostDbUpdateJob() {
  }

  public HostDbUpdateJob(Configuration conf) {
    setConf(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public void updateHosts(boolean buildLinkDb) throws Exception {

    if (buildLinkDb) {
      FIELDS.add(WebPage.Field.INLINKS);
      FIELDS.add(WebPage.Field.OUTLINKS);
    }

    NutchJob job = new NutchJob(getConf(), "hostdb-update");

    // === Map ===
    DataStore<String, WebPage> pageStore = StorageUtils.createWebStore(
        job.getConfiguration(), String.class, WebPage.class);
    Query<String, WebPage> query = pageStore.newQuery();
    query.setFields(StorageUtils.toStringArray(FIELDS)); // Note: pages without
                                                         // these fields are
                                                         // skipped
    GoraMapper.initMapperJob(job, query, pageStore, Text.class, WebPage.class,
        HostDbUpdateJob.Mapper.class, null, true);

    // === Reduce ===
    DataStore<String, Host> hostStore = StorageUtils.createWebStore(
        job.getConfiguration(), String.class, Host.class);
    GoraReducer.initReducerJob(job, hostStore, HostDbUpdateReducer.class);

    job.waitForCompletion(true);
  }

  @Override
  public int run(String[] args) throws Exception {
    boolean linkDb=false;
    for (int i = 0; i < args.length; i++) {
      if ("-linkDb".equals(args[i])) {
        linkDb = true;
      } else if ("-crawlId".equals(args[i])) {
        getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
      }
      else {
        throw new IllegalArgumentException("unrecognized arg " + args[i] 
            + " usage: (-linkDb) (-crawlId <crawlId>)");
      }
    }
    LOG.info("Updating HostDb. Adding links:" + linkDb);
    updateHosts(linkDb);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new HostDbUpdateJob(), args);
    System.exit(res);
  }
}

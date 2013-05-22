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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.storage.Host;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;

/**
 * Display entries from the hostDB. Allows to verify that the storage is OK.
 **/

public class HostDbReader extends Configured implements Tool {
  public static final Log LOG = LogFactory.getLog(HostDbReader.class);

  private void read(String key) throws ClassNotFoundException, IOException, Exception {

    DataStore<String, Host> datastore = StorageUtils.createWebStore(getConf(),
        String.class, Host.class);

    Query<String, Host> query = datastore.newQuery();
    // possibly add a contraint to the query
    if (key != null) {
      query.setKey(TableUtil.reverseUrl(key));
    }
    // query.setFields(Host._ALL_FIELDS);
    Result<String, Host> result = datastore.execute(query);

    while (result.next()) {
      try {
        String hostName = TableUtil.unreverseUrl(result.getKey());
        Host host = result.get();
        System.out.println(hostName);
        System.out.println(host);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    result.close();
    datastore.close();
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new HostDbReader(),
        args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length > 1) {
      System.err.println("Usage: HostDBReader [key]");
      return -1;
    }
    try {
      String key = null;
      if (args.length == 1)
        key = args[0];
      read(key);
      return 0;
    } catch (Exception e) {
      LOG.fatal("HostDBReader: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

}

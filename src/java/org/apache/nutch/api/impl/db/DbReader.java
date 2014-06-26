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
package org.apache.nutch.api.impl.db;

import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.api.model.request.DbFilter;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;

public class DbReader {
  private DataStore<String, WebPage> store;

  public DbReader(Configuration conf, String crawlId) {
    conf = new Configuration(conf);
    if (crawlId != null) {
      conf.set(Nutch.CRAWL_ID_KEY, crawlId);
    }
    try {
      store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot create webstore!", e);
    }
  }

  public Iterator<Map<String, Object>> runQuery(DbFilter filter) {
    String startKey = filter.getStartKey();
    String endKey = filter.getEndKey();

    if (!filter.isKeysReversed()) {
      startKey = reverseKey(filter.getStartKey());
      endKey = reverseKey(filter.getEndKey());
    }

    Query<String, WebPage> query = store.newQuery();
    query.setFields(prepareFields(filter.getFields()));
    if (startKey != null) {
      query.setStartKey(startKey);
      if (endKey != null) {
        query.setEndKey(endKey);
      }
    }
    Result<String, WebPage> result = store.execute(query);
    return new DbIterator(result, filter.getFields(), filter.getBatchId());
  }

  private String reverseKey(String key) {
    if (StringUtils.isEmpty(key)) {
      return null;
    }

    try {
      return TableUtil.reverseUrl(key);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Wrong url format!", e);
    }
  }

  private String[] prepareFields(Set<String> fields) {
    if (CollectionUtils.isEmpty(fields)) {
      return null;
    }
    fields.remove("url");
    return fields.toArray(new String[fields.size()]);
  }
}

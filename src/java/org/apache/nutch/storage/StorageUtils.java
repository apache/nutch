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
package org.apache.nutch.storage;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.Query;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.nutch.metadata.Nutch;

/**
 * Entry point to Gora store/mapreduce functionality.
 * Translates the concept of "crawlid" to the corresponding Gora support.
 */
public class StorageUtils {

  /** Creates a store for the given persistentClass.
   * Currently supports Webpage and Host stores.
   * 
   * @param conf
   * @param keyClass
   * @param persistentClass
   * @return
   * @throws ClassNotFoundException
   * @throws GoraException
   */
  @SuppressWarnings("unchecked")
  public static <K, V extends Persistent> DataStore<K, V> createWebStore(Configuration conf,
      Class<K> keyClass, Class<V> persistentClass) throws ClassNotFoundException, GoraException {
    
    String schema = null;
    if (WebPage.class.equals(persistentClass)) {
      schema = conf.get("storage.schema.webpage", "webpage");
    } else if (Host.class.equals(persistentClass)) {
      schema = conf.get("storage.schema.host", "host");
    } else {
      throw new UnsupportedOperationException("Unable to create store for class " + persistentClass);
    }
    
    String crawlId = conf.get(Nutch.CRAWL_ID_KEY, "");
    
    if (!crawlId.isEmpty()) {
      conf.set("schema.prefix", crawlId + "_");
    } else {
      conf.set("schema.prefix", "");
    }

    Class<? extends DataStore<K, V>> dataStoreClass =
      (Class<? extends DataStore<K, V>>) getDataStoreClass(conf);
    return DataStoreFactory.createDataStore(dataStoreClass,
            keyClass, persistentClass, conf, schema);
  }
  
  /**
   * Return the Persistent Gora class used to persist Nutch Web data.
   * 
   * @param the Nutch configuration 
   * @return the Gora DataStore persistent class
   * @throws ClassNotFoundException
   */
  @SuppressWarnings("unchecked")
  public static <K, V extends Persistent> Class<? extends DataStore<K, V>>
  getDataStoreClass(Configuration conf)  throws ClassNotFoundException {
    return (Class<? extends DataStore<K, V>>)
      Class.forName(conf.get("storage.data.store.class",
          "org.apache.gora.sql.store.SqlStore"));
  }

  public static <K, V> void initMapperJob(Job job,
      Collection<WebPage.Field> fields,
      Class<K> outKeyClass, Class<V> outValueClass,
      Class<? extends GoraMapper<String, WebPage, K, V>> mapperClass)
  throws ClassNotFoundException, IOException {
    initMapperJob(job, fields, outKeyClass, outValueClass,
        mapperClass, null, true);
  }

  public static <K, V> void initMapperJob(Job job,
      Collection<WebPage.Field> fields,
      Class<K> outKeyClass, Class<V> outValueClass,
      Class<? extends GoraMapper<String, WebPage, K, V>> mapperClass,
      Class<? extends Partitioner<K, V>> partitionerClass)
  throws ClassNotFoundException, IOException {
    initMapperJob(job, fields, outKeyClass, outValueClass,
        mapperClass, partitionerClass, true);
  }

  public static <K, V> void initMapperJob(Job job,
      Collection<WebPage.Field> fields,
      Class<K> outKeyClass, Class<V> outValueClass,
      Class<? extends GoraMapper<String, WebPage, K, V>> mapperClass,
      Class<? extends Partitioner<K, V>> partitionerClass, boolean reuseObjects)
  throws ClassNotFoundException, IOException {
    DataStore<String, WebPage> store = createWebStore(job.getConfiguration(),
        String.class, WebPage.class);
    if (store==null) throw new RuntimeException("Could not create datastore");
    Query<String, WebPage> query = store.newQuery();
    query.setFields(toStringArray(fields));
    GoraMapper.initMapperJob(job, query, store,
        outKeyClass, outValueClass, mapperClass, partitionerClass, reuseObjects);
    GoraOutputFormat.setOutput(job, store, true);
  }

  public static <K, V> void initReducerJob(Job job,
      Class<? extends GoraReducer<K, V, String, WebPage>> reducerClass)
  throws ClassNotFoundException, GoraException {
    Configuration conf = job.getConfiguration();
    DataStore<String, WebPage> store =
      StorageUtils.createWebStore(conf, String.class, WebPage.class);
    GoraReducer.initReducerJob(job, store, reducerClass);
    GoraOutputFormat.setOutput(job, store, true);
  }

  public static String[] toStringArray(Collection<WebPage.Field> fields) {
    String[] arr = new String[fields.size()];
    Iterator<WebPage.Field> iter = fields.iterator();
    for (int i = 0; i < arr.length; i++) {
      arr[i] = iter.next().getName();
    }
    return arr;
  }
}

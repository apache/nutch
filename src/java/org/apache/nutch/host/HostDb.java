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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.gora.store.DataStore;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.Host;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.util.TableUtil;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * A caching wrapper for the host datastore. 
 */
public class HostDb implements Closeable {
  public static final Log LOG = LogFactory.getLog(HostDb.class);
  
  private static final class CacheHost {
    private final Host host;
    private final long timestamp;
    public CacheHost(Host host, long timestamp) {
      this.host = host;
      this.timestamp = timestamp;
    }   
  }
  private final static CacheHost NULL_HOST = new CacheHost(null,0);
  

  private DataStore<String, Host> hostStore;

  public static final String HOSTDB_LRU_SIZE = "hostdb.lru.size";
  public static final int DEFAULT_LRU_SIZE = 100;
  public static final String HOSTDB_CONCURRENCY_LEVEL = "hostdb.concurrency.level";
  public static final int DEFAULT_HOSTDB_CONCURRENCY_LEVEL = 8;

  private Cache<String, CacheHost> cache;
  
  private AtomicLong lastFlush;

  public HostDb(Configuration conf) throws GoraException {
    try {
      hostStore = StorageUtils.createWebStore(conf, String.class, Host.class);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    // Create a cache.
    // We add a removal listener to see if we need to flush the store,
    // in order to adhere to the put-flush-get semantic 
    // ("read your own write") of DataStore.
    
    long lruSize = conf.getLong(HOSTDB_LRU_SIZE, DEFAULT_LRU_SIZE);
    int concurrencyLevel = conf.getInt(HOSTDB_CONCURRENCY_LEVEL, 
        DEFAULT_HOSTDB_CONCURRENCY_LEVEL);
    RemovalListener<String, CacheHost> listener = 
        new RemovalListener<String, CacheHost>() {
          @Override
          public void onRemoval(
              RemovalNotification<String, CacheHost> notification) {
            CacheHost removeFromCacheHost = notification.getValue();
            if (removeFromCacheHost != NULL_HOST) {
              if (removeFromCacheHost.timestamp < lastFlush.get()) {
                try {
                  hostStore.flush();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
                lastFlush.set(System.currentTimeMillis());
              }
            }
          }
    };
    
    cache=CacheBuilder.newBuilder().maximumSize(lruSize)
        .removalListener(listener).concurrencyLevel(concurrencyLevel)
        .build();
    lastFlush = new AtomicLong(System.currentTimeMillis());
  }

  
  
  public Host get(final String key) throws IOException {
    Callable<CacheHost> valueLoader = new Callable<CacheHost>() {
      @Override
      public CacheHost call() throws Exception {
        Host host = hostStore.get(key);
        if (host == null) return NULL_HOST;
        return new CacheHost(host, System.currentTimeMillis());
      }  
    };
    CacheHost cachedHost;
    try {
      cachedHost = cache.get(key, valueLoader);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
    if (cachedHost != NULL_HOST) {
      return cachedHost.host;
    } else {
      return null;
    }
  }
 


  public Host getByHostName(String hostName) throws IOException {
   return get(TableUtil.reverseHost(hostName));
  }
  
  
  public void put(String key, Host host) throws IOException {
    cache.put(key, new CacheHost(host, System.currentTimeMillis()));
    hostStore.put(key, host);
  }

  @Override
  public void close() throws IOException {
    hostStore.close();
  }
}

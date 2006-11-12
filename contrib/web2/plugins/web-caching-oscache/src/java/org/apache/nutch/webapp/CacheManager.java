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
package org.apache.nutch.webapp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.webapp.common.Search;
import org.apache.nutch.webapp.common.ServiceLocator;

import com.opensymphony.oscache.base.Cache;
import com.opensymphony.oscache.base.CacheEntry;
import com.opensymphony.oscache.base.EntryRefreshPolicy;
import com.opensymphony.oscache.base.NeedsRefreshException;
import com.opensymphony.oscache.general.GeneralCacheAdministrator;

/**
 * This class is responsible for configuring the used cache and
 * delivering cached Search objects.
 * 
 * Configuration parameters can be overrided with default nutch
 * configuration mechanism.
 * 
 * Search Objects are compressed for smaller space requirements.
 */
public class CacheManager {
  
  public static class ByteBufferWrapper implements Serializable {
    
    private static final long serialVersionUID = 1L;
    byte[] contents;
    
    public ByteBufferWrapper(final byte[] contents){
      this.contents=contents;
    }
    
    public byte[] getContents(){
      return contents;
    }
    
  }

  static final Log LOG=LogFactory.getLog(CacheManager.class);
  
  static final String CACHE_KEY=CacheManager.class.getName();

  class NutchRefreshPolicy implements EntryRefreshPolicy {

    private static final long serialVersionUID = 1L;

    public boolean needsRefresh(CacheEntry arg0) {
      return false;
    }
  }
  
  EntryRefreshPolicy policy=new NutchRefreshPolicy();


  Cache cache;
  GeneralCacheAdministrator cacheadmin;
  
  protected CacheManager(Configuration conf){
    
    Properties p=new Properties();
    
    //use memory for caching
    boolean cacheMemory=conf.getBoolean("cache.memory", false);
    p.setProperty("cache.memory", Boolean.toString(cacheMemory));
    
    //the persistence class used
    String cachePersistenceClass=conf.get("cache.persistence.class","org.apache.nutch.cache.CustomDiskPersistenceListener");
    p.setProperty("cache.persistence.class", cachePersistenceClass);

    //where to store cache files (if file cache used)
    String cachePath=conf.get("cache.path", ".");
    p.setProperty("cache.path", cachePath);

    //cacacity of cache (how many entries)
    int cacheCapacity=conf.getInt("cache.capacity", 1000);
    p.setProperty("cache.capacity", Integer.toString(cacheCapacity));
    
    cacheadmin=new GeneralCacheAdministrator(p);
    cache=cacheadmin.getCache();
  }
  
  public synchronized static CacheManager getInstance(Configuration conf){
    CacheManager cache=(CacheManager)conf.getObject(CACHE_KEY);
    
    if(cache==null) {
      cache = new CacheManager(conf);
      
      conf.setObject(CACHE_KEY, cache);
    }
    return cache;
  }
  
  /**
   * Get Search object from cache
   * @param id key
   * @return
   * @throws NeedsRefreshException
   */
  public Search getSearch(String id, ServiceLocator locator) throws NeedsRefreshException  {
    Search search=null;
    
    ByteBufferWrapper w=(ByteBufferWrapper)cache.getFromCache(id);
    if(w!=null){
      

      try {
        long time=System.currentTimeMillis();
        ByteArrayInputStream is=new ByteArrayInputStream(w.getContents());
        GZIPInputStream gs = new GZIPInputStream(is);
        DataInputStream dis = new DataInputStream(gs);

        search = new Search(locator);
        search.readFields(dis);
        long delta=System.currentTimeMillis()-time;
        
        if(LOG.isDebugEnabled()){
          LOG.debug("Decompressing cache entry took: " + delta + "ms.");
        }

        search.init();
      } catch (IOException e) {
        LOG.info("Could not get cached object: " + e);
      }
    } 
    return search;
  }

  /**
   * Put Search object in cache
   * 
   * @param id key
   * @param search the search to cache
   */
  public void putSearch(String id, Search search){
    try {
      long time=System.currentTimeMillis();
      ByteArrayOutputStream bos=new ByteArrayOutputStream();
      GZIPOutputStream gzos=new GZIPOutputStream(bos);
      DataOutputStream oos=new DataOutputStream(gzos);
      search.write(oos);
      oos.flush();
      oos.close();
      gzos.close();
      long delta=System.currentTimeMillis()-time;
      ByteBufferWrapper wrap=new ByteBufferWrapper(bos.toByteArray());
      if(LOG.isDebugEnabled()){
        LOG.debug("Compressing cache entry took: " + delta + "ms.");
        LOG.debug("size: " + wrap.getContents().length + " bytes");
      }
      cache.putInCache(id, wrap);
    } catch (IOException e) {
      LOG.info("cannot store object in cache: " + e);
    }
  }

  public void cancelUpdate(String key) {
    cache.cancelUpdate(key);
  }
}

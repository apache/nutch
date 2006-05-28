/*
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.webapp.common.Search;

import com.opensymphony.oscache.base.Cache;
import com.opensymphony.oscache.base.CacheEntry;
import com.opensymphony.oscache.base.EntryRefreshPolicy;
import com.opensymphony.oscache.base.NeedsRefreshException;

/**
 * CacheManager for 
 */
public class CacheManager {

  static final String CACHE_KEY="cache";
  
  class NutchRefreshPolicy implements EntryRefreshPolicy {

    private static final long serialVersionUID = 1L;

    public boolean needsRefresh(CacheEntry arg0) {
      return false;
    }
  }
  
  EntryRefreshPolicy policy=new NutchRefreshPolicy();

  Cache cache;
  
  protected CacheManager(){
    cache=new Cache(true,true,false,true,"com.opensymphony.oscache.base.algorithm.UnlimitedCache",Integer.MAX_VALUE);
  }
  
  public static CacheManager getInstance(Configuration conf){
    CacheManager cache=(CacheManager)conf.getObject(CACHE_KEY);
    
    if(cache==null) {
      cache = new CacheManager();
      
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
  public Search getSearch(String id) throws NeedsRefreshException  {
    return (Search) cache.getFromCache(id);
  }

  /**
   * Put Search object in cache
   * 
   * @param id key
   * @param search the search to cache
   */
  public void putSearch(String id, Search search){
    cache.putInCache(id,search,policy);
  }

}

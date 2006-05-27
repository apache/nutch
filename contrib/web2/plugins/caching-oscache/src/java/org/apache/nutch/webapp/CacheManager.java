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
  
  class NutchReferehPolicy implements EntryRefreshPolicy {
    private static final long serialVersionUID = 1L;

    public boolean needsRefresh(CacheEntry arg0) {
      return false;
    }
  }
  
  EntryRefreshPolicy policy=new NutchReferehPolicy();

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

package org.apache.nutch.api;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

public interface ConfManager {

  public Set<String> list() throws Exception;
  
  public Configuration get(String confId);
  
  public Map<String,String> getAsMap(String confId);
  
  public void delete(String confId);
  
  public void create(String confId, Map<String,String> props, boolean force) throws Exception;
  
  public void setProperty(String confId, String propName, String propValue) throws Exception;
}

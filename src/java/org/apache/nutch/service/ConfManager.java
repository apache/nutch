package org.apache.nutch.service;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.service.model.request.NutchConfig;

public interface ConfManager {

  public Configuration get(String confId);

  public Map<String, String> getAsMap(String confId);

  public void setProperty(String confId, String propName, String propValue);
  
  public Set<String> list();

  public String create(NutchConfig nutchConfig);
  
  public void delete(String confId);
}

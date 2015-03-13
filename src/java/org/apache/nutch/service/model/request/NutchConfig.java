package org.apache.nutch.service.model.request;

import java.util.Map;

import java.util.Collections;

public class NutchConfig {
  private String configId;
  private boolean force = false;
  private Map<String, String> params = Collections.emptyMap();

  public Map<String, String> getParams() {
    return params;
  }

  public void setParams(Map<String, String> params) {
    this.params = params;
  }

  public String getConfigId() {
    return configId;
  }

  public void setConfigId(String configId) {
    this.configId = configId;
  }

  public boolean isForce() {
    return force;
  }

  public void setForce(boolean force) {
    this.force = force;
  }
}

package org.apache.nutch.webui.model;

import java.io.Serializable;

public class NutchConfig implements Serializable {
  private String name = "name";
  private String value;

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }
}

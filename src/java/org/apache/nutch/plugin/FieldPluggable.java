package org.apache.nutch.plugin;

import java.util.Collection;

import org.apache.nutch.storage.WebPage;

public interface FieldPluggable extends Pluggable {
  public Collection<WebPage.Field> getFields();

}

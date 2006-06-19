package org.apache.nutch.cache;

import com.opensymphony.oscache.plugins.diskpersistence.AbstractDiskPersistenceListener;

public class CustomDiskPersistenceListener extends
    AbstractDiskPersistenceListener {

  protected char[] getCacheFileName(String arg0) {
    return arg0.toCharArray();
  }

}

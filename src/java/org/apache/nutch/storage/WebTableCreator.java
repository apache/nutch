package org.apache.nutch.storage;

import org.apache.nutch.util.NutchConfiguration;
import org.gora.store.DataStore;

public class WebTableCreator {
  public static void main(String[] args) throws Exception {
    DataStore<String, WebPage> store =
      StorageUtils.createDataStore(NutchConfiguration.create(), String.class,
        WebPage.class);

    System.out.println(store);
  }
}

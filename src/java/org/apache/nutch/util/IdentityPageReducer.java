package org.apache.nutch.util;

import java.io.IOException;

import org.apache.nutch.storage.WebPage;
import org.apache.gora.mapreduce.GoraReducer;

public class IdentityPageReducer
extends GoraReducer<String, WebPage, String, WebPage> {

  @Override
  protected void reduce(String key, Iterable<WebPage> values,
      Context context) throws IOException, InterruptedException {
    for (WebPage page : values) {
      context.write(key, page);
    }
  }

}

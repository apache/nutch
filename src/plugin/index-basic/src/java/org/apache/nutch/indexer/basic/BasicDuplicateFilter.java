package org.apache.nutch.indexer.basic;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.DuplicateFilter;

public class BasicDuplicateFilter implements DuplicateFilter {
  private Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public boolean isOriginal(String url, List<CharSequence> duplicates) {
    for (CharSequence duplicate : duplicates) {
      if (isLonger(url, duplicate)) {
        return false;
      }
    }
    return true;
  }
  
  private boolean isLonger(String url, CharSequence duplicate) {
    int originalPathSegmentCount = url.split("/").length;
    int duplicatePathSegmentCount = duplicate.toString().split("/").length;
    if (originalPathSegmentCount < duplicatePathSegmentCount) {
      return false;
    } else if (originalPathSegmentCount > duplicatePathSegmentCount) {
      return true;
    } else {
      if (url.length() < duplicate.length()) {
        return false;
      }
    }
    return true;
  }

}

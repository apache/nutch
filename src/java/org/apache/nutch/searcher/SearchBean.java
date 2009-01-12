package org.apache.nutch.searcher;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public interface SearchBean extends Searcher, HitDetailer {
  public static final Log LOG = LogFactory.getLog(SearchBean.class);

  public boolean ping() throws IOException ;
}

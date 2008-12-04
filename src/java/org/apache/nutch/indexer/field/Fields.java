package org.apache.nutch.indexer.field;

public interface Fields {

  // names of common fields
  public static final String ANCHOR = "anchor";
  public static final String SEGMENT = "segment";
  public static final String DIGEST = "digest";
  public static final String HOST = "host";
  public static final String SITE = "site";
  public static final String URL = "url";
  public static final String ORIG_URL = "orig";
  public static final String SEG_URL = "segurl";
  public static final String CONTENT = "content";
  public static final String TITLE = "title";
  public static final String CACHE = "cache";
  public static final String TSTAMP = "tstamp";
  public static final String BOOSTFACTOR = "boostfactor";
  
  // special fields for indexer
  public static final String BOOST = "boost";
  public static final String COMPUTATION = "computation";
  public static final String ACTION = "action";
}

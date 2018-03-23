package org.apache.nutch.crawl;

public enum SitemapOperation {
  NONE,
  ONLY,
  ALL;
  
  public static SitemapOperation get(String operation) {
    switch (operation.toUpperCase()) {
    case "NONE":
      return NONE;
    case "ONLY":
      return ONLY;
    case "ALL":
      return ALL;
    default:
      throw new RuntimeException("Operation "+operation+" not recognized");
    }
  }
}

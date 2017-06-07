package org.apache.nutch.core.jsoup.extractor.normalizer;

public class SimpleStringNormalizer implements Normalizable {
  
  private int LENGTH_LIMIT = 32766;

  @Override
  public String normalize(String content) {
    content = content.trim();
    content = content.substring(0, Math.min(LENGTH_LIMIT, content.length()));
    return content;
  }

}

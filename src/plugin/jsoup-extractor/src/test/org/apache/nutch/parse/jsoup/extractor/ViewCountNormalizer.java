package org.apache.nutch.parse.jsoup.extractor;

import org.apache.nutch.jsoup.extractor.core.normalizer.Normalizable;

public class ViewCountNormalizer implements Normalizable {

  @Override
  public String normalize(String viewCountStr) {
    String viewCount = viewCountStr.split("\\s+")[0];
    return viewCount.replaceAll(",", "");
  }

}

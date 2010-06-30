package org.apache.nutch.crawl;

import org.apache.nutch.storage.WebPage;

public class URLWebPage {

  private String url;

  private WebPage datum;

  public URLWebPage(String url, WebPage datum) {
    this.url = url;
    this.datum = datum;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public WebPage getDatum() {
    return datum;
  }

  public void setDatum(WebPage datum) {
    this.datum = datum;
  }

}

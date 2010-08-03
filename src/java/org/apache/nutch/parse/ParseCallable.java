package org.apache.nutch.parse;

import java.util.concurrent.Callable;

import org.apache.nutch.storage.WebPage;

class ParseCallable implements Callable<Parse> {
  private Parser p;
  private WebPage content;
  private String url;
  
  public ParseCallable(Parser p, WebPage content, String url) {
    this.p = p;
    this.content = content;
    this.url = url;
  }

  @Override
  public Parse call() throws Exception {
    return p.getParse(url, content);
  }    
}
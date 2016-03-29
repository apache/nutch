package org.apache.nutch.protocol.htmlunit;

import com.gargoylesoftware.htmlunit.WebWindowEvent;
import com.gargoylesoftware.htmlunit.WebWindowListener;

public class HtmlUnitWebWindowListener implements WebWindowListener {

  private Integer redirectCount = 0;
  private Integer maxRedirects = 0;
  
  public HtmlUnitWebWindowListener() {
    
  }
  
  public HtmlUnitWebWindowListener(int maxRedirects) {
    this.maxRedirects = maxRedirects;
  }
  
  @Override
  public void webWindowOpened(WebWindowEvent event) {
    
  }

  @Override
  public void webWindowContentChanged(WebWindowEvent event) {
    redirectCount++;
    if(redirectCount > maxRedirects)
      throw new RuntimeException("Redirect Count: " + redirectCount + " exceeded the Maximum Redirects allowed: " + maxRedirects);
  }

  @Override
  public void webWindowClosed(WebWindowEvent event) {
    
  }
  
}


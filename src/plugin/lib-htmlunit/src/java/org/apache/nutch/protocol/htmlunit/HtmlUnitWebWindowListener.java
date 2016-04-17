/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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


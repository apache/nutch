/**
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
package org.apache.nutch.protocol.interactiveselenium.handlers;

import org.apache.hadoop.util.StringUtils;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a placeholder/example of a technique or use case where we do multiple 
 * interaction with the web driver and need data from each such interaction in the end. This code shows that after you have 
 * done multiple interactions and accumulated data you can in the end append that to the driver.  
 */
public class DefalultMultiInteractionHandler implements
    InteractiveSeleniumHandler {
  private static final Logger LOG = LoggerFactory
      .getLogger(DefalultMultiInteractionHandler.class);

  public void processDriver(WebDriver driver) {
    try {
      // loop and get multiple pages in this string
      String accumulatedData = "";
      // append the string to the last page's driver
      JavascriptExecutor jsx = (JavascriptExecutor) driver;
      jsx.executeScript("document.body.innerHTML=document.body.innerHTML "
          + accumulatedData + ";");
    } catch (Exception e) {
      LOG.info(StringUtils.stringifyException(e));
    }
  }

  public boolean shouldProcessURL(String URL) {
    return true;
  }
}

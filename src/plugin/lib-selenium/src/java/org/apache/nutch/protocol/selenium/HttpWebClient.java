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
package org.apache.nutch.protocol.selenium;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxProfile;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.lang.String;

public class HttpWebClient {

  private static final Logger LOG = LoggerFactory.getLogger("org.apache.nutch.protocol");

  public static ThreadLocal<WebDriver> threadWebDriver = new ThreadLocal<WebDriver>() {

    @Override
    protected WebDriver initialValue()
    {
      FirefoxProfile profile = new FirefoxProfile();
      profile.setPreference("permissions.default.stylesheet", 2);
      profile.setPreference("permissions.default.image", 2);
      profile.setPreference("dom.ipc.plugins.enabled.libflashplayer.so", "false");
      WebDriver driver = new FirefoxDriver(profile);
      return driver;
    };
  };

  public static String getHtmlPage(String url, Configuration conf) {
    WebDriver driver = null;

    try {
      driver = new FirefoxDriver();
      //} WebDriver driver = threadWebDriver.get();
      //  if (driver == null) {
      //    driver = new FirefoxDriver();
      //  }

      driver.get(url);

      // Wait for the page to load, timeout after 3 seconds
      new WebDriverWait(driver, 3);

      String innerHtml = driver.findElement(By.tagName("body")).getAttribute("innerHTML");

      return innerHtml;

      // I'm sure this catch statement is a code smell ; borrowing it from lib-htmlunit
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (driver != null) try { driver.quit(); } catch (Exception e) { throw new RuntimeException(e); }
    }
  };

  public static String getHtmlPage(String url) {
    return getHtmlPage(url, null);
  }
}
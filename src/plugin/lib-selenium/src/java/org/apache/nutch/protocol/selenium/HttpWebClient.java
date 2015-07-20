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
package org.apache.nutch.protocol.selenium;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.openqa.selenium.By;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxProfile;
import org.openqa.selenium.safari.SafariDriver;
import org.openqa.selenium.support.ui.WebDriverWait;
import com.opera.core.systems.OperaDriver;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.String;

public class HttpWebClient {

  private static final Logger LOG = LoggerFactory.getLogger(HttpWebClient.class);

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

  public static WebDriver getDriverForPage(String url, Configuration conf) {
      WebDriver driver = null;
      long pageLoadWait = conf.getLong("libselenium.page.load.delay", 3);

      try {
          driver = new FirefoxDriver();
          driver.get(url);
          new WebDriverWait(driver, pageLoadWait);
      } catch (Exception e) {
          throw new RuntimeException(e);
      }

      return driver;
  }

  public static String getHTMLContent(WebDriver driver) {
      return driver.findElement(By.tagName("body")).getAttribute("innerHTML");
  }

  public static void cleanUpDriver(WebDriver driver) {
      if (driver != null) {
          try {
              driver.quit();
          } catch (Exception e) {
              throw new RuntimeException(e);
          }
      }
  }

  /**
   * Function for obtaining the HTML BODY using the selected
   * {@link org.openqa.selenium.WebDriver}.
   * There are a number of configuration properties within
   * <code>nutch-site.xml</code> which determine whether to
   * take screenshots of the rendered pages and persist them
   * as timestamped .png's into HDFS.
   * @param url the URL to fetch and render
   * @param conf the {@link org.apache.hadoop.conf.Configuration}
   * @return the rendered inner HTML page
   */
  public static String getHtmlPage(String url, Configuration conf) {
    long pageLoadWait = conf.getLong("libselenium.page.load.delay", 3);
    WebDriver driver = null;
    try {
      String driverType  = conf.get("selenium.driver", "firefox");
      switch (driverType) {
      case "firefox":
        driver = new FirefoxDriver();
        break;
      case "chrome":
        driver = new ChromeDriver();
        break;
      case "safari":
        driver = new SafariDriver();
        break;
      case "opera":
        driver = new OperaDriver();
        break;
      default:
        LOG.error("The Selenium WebDriver choice {} is not available... defaulting to FirefoxDriver().", driverType);
        driver = new FirefoxDriver();
        break;
      }
      LOG.debug("Selenium {} WebDriver selected.", driverType);
      driver.get(url);

      new WebDriverWait(driver, pageLoadWait);

      if (conf.getBoolean("selenium.take.screenshot", false)) {
        File srcFile = ((TakesScreenshot)driver).getScreenshotAs(OutputType.FILE);
        LOG.debug("In-memory screenshot taken of: {}", url);
        FileSystem fs = FileSystem.get(conf);
        Path screenshotPath = new Path(conf.get("selenium.screenshot.location") + "/" + srcFile.getName());
        if (screenshotPath != null) {
          OutputStream os = null;
          if (!fs.exists(screenshotPath)) {
            LOG.debug("No existing screenshot already exists... creating new file at {} {}.", screenshotPath, srcFile.getName());
            os = fs.create(screenshotPath);
          }
          InputStream is = new BufferedInputStream(new FileInputStream(srcFile));
          IOUtils.copyBytes(is, os, conf);
          LOG.debug("Screenshot for {} successfully saved to: {} {}", url, screenshotPath, srcFile.getName()); 
        } else {
          LOG.warn("Screenshot for {} not saved to HDFS (subsequently disgarded) as value for "
              + "'selenium.screenshot.location' is absent from nutch-site.xml.", url);
        }
      }

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

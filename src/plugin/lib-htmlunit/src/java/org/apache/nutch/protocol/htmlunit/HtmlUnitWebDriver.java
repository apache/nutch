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

import java.lang.invoke.MethodHandles;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;
import org.openqa.selenium.io.TemporaryFilesystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gargoylesoftware.htmlunit.WebClient;

public class HtmlUnitWebDriver extends HtmlUnitDriver {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  private static boolean enableJavascript;
  private static boolean enableCss;
  private static boolean enableRedirect;
  private static long javascriptTimeout;
  private static int maxRedirects;
  
  public HtmlUnitWebDriver() {
    super(enableJavascript);
  }
  
  @Override
  protected WebClient modifyWebClient(WebClient client) {
    client.getOptions().setJavaScriptEnabled(enableJavascript);
    client.getOptions().setCssEnabled(enableCss);
    client.getOptions().setRedirectEnabled(enableRedirect);
    if(enableJavascript)
      client.setJavaScriptTimeout(javascriptTimeout);
      client.getOptions().setThrowExceptionOnScriptError(false);
      if(enableRedirect)
        client.addWebWindowListener(new HtmlUnitWebWindowListener(maxRedirects));
	  return client;
  }
  
  public static WebDriver getDriverForPage(String url, Configuration conf) {
    long pageLoadTimout = conf.getLong("page.load.delay", 3);
    enableJavascript = conf.getBoolean("htmlunit.enable.javascript", true);
    enableCss = conf.getBoolean("htmlunit.enable.css", false);
    javascriptTimeout = conf.getLong("htmlunit.javascript.timeout", 3500);
    int redirects = Integer.parseInt(conf.get("http.redirect.max", "0"));
    enableRedirect = redirects <= 0 ? false : true;
    maxRedirects = redirects;
	  
    WebDriver driver = null;
	  
    try {
      driver = new HtmlUnitWebDriver();
      driver.manage().timeouts().pageLoadTimeout(pageLoadTimout, TimeUnit.SECONDS);
      driver.get(url);
     } catch(Exception e) {
       if(e instanceof TimeoutException) {
	       LOG.debug("HtmlUnit WebDriver: Timeout Exception: Capturing whatever loaded so far...");
	       return driver;
     }
     cleanUpDriver(driver);
     throw new RuntimeException(e);
    }

    return driver;
  }

  public static String getHTMLContent(WebDriver driver, Configuration conf) {
    try {
      if (conf.getBoolean("take.screenshot", false))
        takeScreenshot(driver, conf);
		  
      String innerHtml = "";
      if(enableJavascript) {
	      WebElement body = driver.findElement(By.tagName("body"));
	      innerHtml = (String)((JavascriptExecutor)driver).executeScript("return arguments[0].innerHTML;", body); 
      }
      else
	      innerHtml = driver.getPageSource().replaceAll("&amp;", "&");
      return innerHtml;
    } catch(Exception e) {
	    TemporaryFilesystem.getDefaultTmpFS().deleteTemporaryFiles();
    	cleanUpDriver(driver);
    	throw new RuntimeException(e);
    } 
  }

  public static void cleanUpDriver(WebDriver driver) {
    if (driver != null) {
      try {
        driver.close();
        driver.quit();
        TemporaryFilesystem.getDefaultTmpFS().deleteTemporaryFiles();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Function for obtaining the HTML BODY using the selected
   * <a href='https://seleniumhq.github.io/selenium/docs/api/java/org/openqa/selenium/WebDriver.html'>selenium webdriver</a>
   * There are a number of configuration properties within
   * <code>nutch-site.xml</code> which determine whether to
   * take screenshots of the rendered pages and persist them
   * as timestamped .png's into HDFS.
   * @param url the URL to fetch and render
   * @param conf the {@link org.apache.hadoop.conf.Configuration}
   * @return the rendered inner HTML page
   */
  public static String getHtmlPage(String url, Configuration conf) {
    WebDriver driver = getDriverForPage(url, conf);

    try {
      if (conf.getBoolean("take.screenshot", false))
	      takeScreenshot(driver, conf);

      String innerHtml = "";
      if(enableJavascript) {
	      WebElement body = driver.findElement(By.tagName("body"));
    	  innerHtml = (String)((JavascriptExecutor)driver).executeScript("return arguments[0].innerHTML;", body); 
      }
      else
    	  innerHtml = driver.getPageSource().replaceAll("&amp;", "&");
      return innerHtml;

    } catch (Exception e) {
	    TemporaryFilesystem.getDefaultTmpFS().deleteTemporaryFiles();
      throw new RuntimeException(e);
    } finally {
      cleanUpDriver(driver);
    }
  }

  private static void takeScreenshot(WebDriver driver, Configuration conf) {
    try {
      String url = driver.getCurrentUrl();
      File srcFile = ((TakesScreenshot)driver).getScreenshotAs(OutputType.FILE);
      LOG.debug("In-memory screenshot taken of: {}", url);
      FileSystem fs = FileSystem.get(conf);
      if (conf.get("screenshot.location") != null) {
    	  Path screenshotPath = new Path(conf.get("screenshot.location") + "/" + srcFile.getName());
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
            + "'screenshot.location' is absent from nutch-site.xml.", url);
      }
    } catch (Exception e) {
    	cleanUpDriver(driver);
    	throw new RuntimeException(e);
    }
  }
}

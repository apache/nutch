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

import java.lang.invoke.MethodHandles;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxBinary;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;
import org.openqa.selenium.firefox.FirefoxProfile;
import org.openqa.selenium.io.TemporaryFilesystem;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.safari.SafariDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.openqa.selenium.opera.OperaDriver;
import org.openqa.selenium.opera.OperaOptions;
public class HttpWebClient {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static ThreadLocal<WebDriver> threadWebDriver = new ThreadLocal<WebDriver>() {

    @Override
    protected WebDriver initialValue()
    {
      FirefoxOptions options = new FirefoxOptions();
      FirefoxProfile profile = new FirefoxProfile();
      profile.setPreference("permissions.default.stylesheet", 2);
      profile.setPreference("permissions.default.image", 2);
      profile.setPreference("dom.ipc.plugins.enabled.libflashplayer.so", "false");
      profile.setPreference(FirefoxProfile.ALLOWED_HOSTS_PREFERENCE, "localhost");
      options.setProfile(profile);
      WebDriver driver = new FirefoxDriver(options);
      return driver;          
    };
  };

  public static WebDriver getDriverForPage(String url, Configuration conf) {
      WebDriver driver = null;
      DesiredCapabilities capabilities = null;
      long pageLoadWait = conf.getLong("page.load.delay", 3);

      try {
        String driverType  = conf.get("selenium.driver", "firefox");
        switch (driverType) {
          case "firefox":
          	String allowedHost = conf.get("selenium.firefox.allowed.hosts", "localhost");
          	long firefoxBinaryTimeout = conf.getLong("selenium.firefox.binary.timeout", 45);
          	boolean enableFlashPlayer = conf.getBoolean("selenium.firefox.enable.flash", false);
            boolean headless = conf.getBoolean("selenium.firefox.headless", false);
          	int loadImage = conf.getInt("selenium.firefox.load.image", 1);
          	int loadStylesheet = conf.getInt("selenium.firefox.load.stylesheet", 1);
            FirefoxOptions options = new FirefoxOptions();
            FirefoxProfile profile = new FirefoxProfile();
            FirefoxBinary binary = new FirefoxBinary();
            if(headless) {
              binary.addCommandLineOptions("--headless");
            }
            profile.setPreference(FirefoxProfile.ALLOWED_HOSTS_PREFERENCE, allowedHost);
            profile.setPreference("dom.ipc.plugins.enabled.libflashplayer.so", enableFlashPlayer);
            profile.setPreference("permissions.default.stylesheet", loadStylesheet);
            profile.setPreference("permissions.default.image", loadImage);
            binary.setTimeout(TimeUnit.SECONDS.toMillis(firefoxBinaryTimeout));
            options.setProfile(profile);
            options.setBinary(binary);
            driver = new FirefoxDriver(options);
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
          case "phantomjs":
            driver = new PhantomJSDriver();
            break;
          case "remote":
            String seleniumHubHost = conf.get("selenium.hub.host", "localhost");
            int seleniumHubPort = Integer.parseInt(conf.get("selenium.hub.port", "4444"));
            String seleniumHubPath = conf.get("selenium.hub.path", "/wd/hub");
            String seleniumHubProtocol = conf.get("selenium.hub.protocol", "http");
            String seleniumGridDriver = conf.get("selenium.grid.driver","firefox");
            String seleniumGridBinary = conf.get("selenium.grid.binary");

            switch (seleniumGridDriver){
              case "firefox":
                capabilities = DesiredCapabilities.firefox();
                capabilities.setBrowserName("firefox");
                capabilities.setJavascriptEnabled(true);
                capabilities.setCapability("firefox_binary",seleniumGridBinary);
                System.setProperty("webdriver.reap_profile", "false");
                driver = new RemoteWebDriver(new URL(seleniumHubProtocol, seleniumHubHost, seleniumHubPort, seleniumHubPath), capabilities);
                break;
              case "phantomjs":
                capabilities = DesiredCapabilities.phantomjs();
                capabilities.setBrowserName("phantomjs");
                capabilities.setJavascriptEnabled(true);
                capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY,seleniumGridBinary);
                driver = new RemoteWebDriver(new URL(seleniumHubProtocol, seleniumHubHost, seleniumHubPort, seleniumHubPath), capabilities);
                break;
              default:
                LOG.error("The Selenium Grid WebDriver choice {} is not available... defaulting to FirefoxDriver().", driverType);
                driver = new RemoteWebDriver(new URL(seleniumHubProtocol, seleniumHubHost, seleniumHubPort, seleniumHubPath), DesiredCapabilities.firefox());
                break;
            }
            break;
          default:
            LOG.error("The Selenium WebDriver choice {} is not available... defaulting to FirefoxDriver().", driverType);
            driver = new FirefoxDriver();
            break;
        }
        LOG.debug("Selenium {} WebDriver selected.", driverType);
  
        driver.manage().timeouts().pageLoadTimeout(pageLoadWait, TimeUnit.SECONDS);
        driver.get(url);
      } catch (Exception e) {
			  if(e instanceof TimeoutException) {
          LOG.debug("Selenium WebDriver: Timeout Exception: Capturing whatever loaded so far...");
          return driver;
			  }
			  cleanUpDriver(driver);
		    throw new RuntimeException(e);
	    } 

      return driver;
  }

  public static String getHTMLContent(WebDriver driver, Configuration conf) {
      if (conf.getBoolean("take.screenshot", false)) {
        takeScreenshot(driver, conf);
      }

      return driver.findElement(By.tagName("body")).getAttribute("innerHTML");
  }

  public static void cleanUpDriver(WebDriver driver) {
    if (driver != null) {
      try {
//	      driver.close();
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
      if (conf.getBoolean("take.screenshot", false)) {
        takeScreenshot(driver, conf);
      }

      String innerHtml = driver.findElement(By.tagName("body")).getAttribute("innerHTML");
      return innerHtml;

      // I'm sure this catch statement is a code smell ; borrowing it from lib-htmlunit
    } catch (Exception e) {
      TemporaryFilesystem.getDefaultTmpFS().deleteTemporaryFiles();
      throw new RuntimeException(e);
    } finally {
      cleanUpDriver(driver);
    }
  }

  public static String getHtmlPage(String url) {
    return getHtmlPage(url, null);
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

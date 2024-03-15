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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;
import org.openqa.selenium.io.TemporaryFilesystem;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Random;

public class HttpWebClient {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static WebDriver getDriverForPage(String url, Configuration conf) {
    WebDriver driver = null;
    long pageLoadWait = conf.getLong("page.load.delay", 3);

    try {
      String driverType = conf.get("selenium.driver", "firefox");
      boolean enableHeadlessMode = conf.getBoolean("selenium.enable.headless",
          false);

      switch (driverType) {
      case "firefox":
        String geckoDriverPath = conf.get("selenium.grid.binary",
            "/root/geckodriver");
        driver = createFirefoxWebDriver(geckoDriverPath, enableHeadlessMode);
        break;
      case "chrome":
        String chromeDriverPath = conf.get("selenium.grid.binary",
            "/root/chromedriver");
        driver = createChromeWebDriver(chromeDriverPath, enableHeadlessMode);
        break;
      case "remote":
        String seleniumHubHost = conf.get("selenium.hub.host", "localhost");
        int seleniumHubPort = Integer
            .parseInt(conf.get("selenium.hub.port", "4444"));
        String seleniumHubPath = conf.get("selenium.hub.path", "/wd/hub");
        String seleniumHubProtocol = conf.get("selenium.hub.protocol", "http");
        URL seleniumHubUrl = new URL(seleniumHubProtocol, seleniumHubHost,
            seleniumHubPort, seleniumHubPath);

        String seleniumGridDriver = conf.get("selenium.grid.driver", "firefox");

        switch (seleniumGridDriver) {
        case "firefox":
          driver = createFirefoxRemoteWebDriver(seleniumHubUrl,
              enableHeadlessMode);
          break;
        case "chrome":
          driver = createChromeRemoteWebDriver(seleniumHubUrl,
              enableHeadlessMode);
          break;
        case "random":
          driver = createRandomRemoteWebDriver(seleniumHubUrl,
              enableHeadlessMode);
          break;
        default:
          LOG.error(
              "The Selenium Grid WebDriver choice {} is not available... defaulting to FirefoxDriver().",
              driverType);
          driver = createDefaultRemoteWebDriver(seleniumHubUrl,
              enableHeadlessMode);
          break;
        }
        break;
      default:
        LOG.error(
            "The Selenium WebDriver choice {} is not available... defaulting to FirefoxDriver().",
            driverType);
        FirefoxOptions options = new FirefoxOptions();
        driver = new FirefoxDriver(options);
        break;
      }
      LOG.debug("Selenium {} WebDriver selected.", driverType);

      driver.manage().window().maximize();
      driver.manage().deleteAllCookies();
      driver.manage().timeouts().pageLoadTimeout(Duration.of(pageLoadWait,
          ChronoUnit.SECONDS));
      driver.get(url);
    } catch (Exception e) {
      if (e instanceof TimeoutException) {
        LOG.error(
            "Selenium WebDriver: Timeout Exception: Capturing whatever loaded so far...");
        return driver;
      } else {
        LOG.error(e.toString());
      }
      cleanUpDriver(driver);
      throw new RuntimeException(e);
    }

    return driver;
  }

  public static WebDriver createFirefoxWebDriver(String firefoxDriverPath,
      boolean enableHeadlessMode) {
    FirefoxOptions firefoxOptions = new FirefoxOptions();
    firefoxOptions.setBinary(firefoxDriverPath);
    if (enableHeadlessMode) {
      firefoxOptions.addArguments("-headless");
    }
    return new FirefoxDriver(firefoxOptions);
  }

  public static WebDriver createChromeWebDriver(String chromeDriverPath,
      boolean enableHeadlessMode) {
    // if not specified, WebDriver will search your path for chromedriver
    ChromeOptions chromeOptions = new ChromeOptions();
    chromeOptions.addArguments("--no-sandbox");
    chromeOptions.addArguments("--disable-extensions");
    chromeOptions.setBinary(chromeDriverPath);
    // be sure to set selenium.enable.headless to true if no monitor attached
    // to your server
    if (enableHeadlessMode) {
      chromeOptions.addArguments("--headless=new");
    }
    return new ChromeDriver(chromeOptions);
  }

  public static RemoteWebDriver createFirefoxRemoteWebDriver(URL seleniumHubUrl,
      boolean enableHeadlessMode) {
    FirefoxOptions firefoxOptions = new FirefoxOptions();
    if (enableHeadlessMode) {
      firefoxOptions.addArguments("-headless");
    }
    return new RemoteWebDriver(seleniumHubUrl,
        firefoxOptions);
  }

  public static RemoteWebDriver createChromeRemoteWebDriver(URL seleniumHubUrl,
      boolean enableHeadlessMode) {
    ChromeOptions chromeOptions = new ChromeOptions();
    if (enableHeadlessMode) {
      chromeOptions.addArguments("--headless=new");
    }
    return new RemoteWebDriver(seleniumHubUrl, chromeOptions);
  }

  public static RemoteWebDriver createRandomRemoteWebDriver(URL seleniumHubUrl,
      boolean enableHeadlessMode) {
    // we consider a possibility of generating only 2 types of browsers: Firefox
    // and
    // Chrome only
    Random r = new Random();
    int min = 0;
    // we have actually hardcoded the maximum number of types of web driver that
    // can
    // be created
    // but this must be later moved to the configuration file in order to be
    // able
    // to randomly choose between much more types(ex: Edge, Opera, Safari)
    int max = 1; // for 3 types, change to 2 and update the if-clause
    int num = r.nextInt((max - min) + 1) + min;
    if (num == 0) {
      return createFirefoxRemoteWebDriver(seleniumHubUrl, enableHeadlessMode);
    }
    return createChromeRemoteWebDriver(seleniumHubUrl, enableHeadlessMode);
  }

  public static RemoteWebDriver createDefaultRemoteWebDriver(URL seleniumHubUrl,
      boolean enableHeadlessMode) {
    return createFirefoxRemoteWebDriver(seleniumHubUrl, enableHeadlessMode);
  }

  public static void cleanUpDriver(WebDriver driver) {
    if (driver != null) {
      try {
        // driver.close();
        driver.quit();
        TemporaryFilesystem.getDefaultTmpFS().deleteTemporaryFiles();
      } catch (Exception e) {
        LOG.error(e.toString());
        // throw new RuntimeException(e);
      }
    }
  }

  /**
   * Function for obtaining the HTML using the selected <a href=
   * 'https://www.selenium.dev/selenium/docs/api/java/org/openqa/selenium/WebDriver.html'>selenium
   * webdriver</a> There are a number of configuration properties within
   * <code>nutch-site.xml</code> which determine whether to take screenshots of
   * the rendered pages and persist them as timestamped .png's into HDFS.
   * 
   * @param url
   *          the URL to fetch and render
   * @param conf
   *          the {@link org.apache.hadoop.conf.Configuration}
   * @return the html page
   */
  public static String getHtmlPage(String url, Configuration conf) {
    WebDriver driver = getDriverForPage(url, conf);

    try {
      if (conf.getBoolean("take.screenshot", false)) {
        takeScreenshot(driver, conf);
      }
      return driver.getPageSource();

      // I'm sure this catch statement is a code smell ; borrowing it from
      // lib-htmlunit
    } catch (Exception e) {
      TemporaryFilesystem.getDefaultTmpFS().deleteTemporaryFiles();
      // throw new RuntimeException(e);
      LOG.error("getHtmlPage(url, conf): {}", e.toString());
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
      File srcFile = ((TakesScreenshot) driver)
          .getScreenshotAs(OutputType.FILE);
      LOG.debug("In-memory screenshot taken of: {}", url);
      FileSystem fs = FileSystem.get(conf);
      if (conf.get("screenshot.location") != null) {
        String screenshotPath = conf.get("screenshot.location", "");
        Path path = new Path(String.valueOf(new File(screenshotPath, srcFile.getName())));
        OutputStream os = null;
        if (!fs.exists(path)) {
          LOG.debug(
              "No existing screenshot already exists... creating new file at {}/{}.",
              screenshotPath, srcFile.getName());
          os = fs.create(path);
        }
        InputStream is = new BufferedInputStream(new FileInputStream(srcFile));
        IOUtils.copyBytes(is, os, conf);
        LOG.debug("Screenshot for {} successfully saved to: {}/{}", url,
            screenshotPath, srcFile.getName());
      } else {
        LOG.warn(
            "Screenshot for {} not saved to HDFS (subsequently discarded) as value for "
                + "'screenshot.location' is absent from nutch-site.xml.",
            url);
      }
    } catch (Exception e) {
      LOG.error("Error taking screenshot: ", e);
      cleanUpDriver(driver);
      throw new RuntimeException(e);
    }
  }
}

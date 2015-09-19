Nutch Selenium
==============

# Introduction

This plugin allows you to fetch Javascript pages using [Selenium](http://www.seleniumhq.org/), while relying on the rest of the awesome Nutch stack!

The underlying code is based on the nutch-htmlunit plugin, which was in turn based on nutch-httpclient.

There are essentially two ways in which Nutch can be used with Selenium.

 * Locally (on each node) as a self contained process, or
 * via the RemoteWebDriver which connects to [Selenium-Grid](http://www.seleniumhq.org/docs/07_selenium_grid.jsp). A grid consists of a single hub, and one or more nodes.

# Installation

## Part 1: 

### A) Setting up Selenium (local mode)

 * Ensure that you have Firefox installed. More info about the package @ [launchpad](https://launchpad.net/ubuntu/trusty/+source/firefox)
```
sudo apt-get install firefox
```
 * Install Xvfb and its associates
```
sudo apt-get install xorg synaptic xvfb gtk2-engines-pixbuf xfonts-cyrillic xfonts-100dpi \
    xfonts-75dpi xfonts-base xfonts-scalable freeglut3-dev dbus-x11 openbox x11-xserver-utils \
    libxrender1 cabextract
```
 * Set a display for Xvfb, so that firefox believes a display is connected
```
sudo /usr/bin/Xvfb :11 -screen 0 1024x768x24 &
sudo export DISPLAY=:11
```
### B) Setting up a Selenium Grid 

Using the Selenium Grid will allow you to parallelize the job by facilitating access of several instances of browsers whether on one machine or on several machines. Note that grid facilitates heterogeneity with regards to browser types used. However, these steps have been tested using a homogenous firefox Selenium Grid. 

 * Download the [Selenium Standalone Server](http://www.seleniumhq.org/download/) and follow the installation instructions.
 
 * Some important configurations to note while setting up the selenium-hub and the selenium-nodes are:
    * For the hub: 
      - maxSession (how many browser sessions to allow on the grid at a time)
      - browserTimeout (how long to wait before timing out a browser session. This is dependent on the interactivity to be completed on the page)
      
    * For the nodes:
      - browserName=<browser>, maxInstances (the max number of instances of the same version browser to allow per a system)
      - browserName=<browser>, maxSession (the max number of sessions of any type of browser/version to allow per a system)
      
  * Go headless with your selenium Grid installation. There are different ways to this. See [this resource](http://elementalselenium.com/tips/38-headless) for further details. 
 
  * For Nutch efficiency, and optimization of the grid, consider editing the following configs in **nutch-site.xml**
    - fetcher.threads.per.queue (change value to the value of the maxSession config on the hub)
    - fetcher.threads.fetch (change value to the value of the maxSession config on the hub)
    - fetcher.server.delay (As multiple threads may be accessing a single server at a time, consider changing this value to 4-5 seconds for politeness)
    - fetcher.server.min.delay (As multiple threads may be accessing a single server at a time, consider changing this values to 4-5 seconds for politeness)
    - Ensure all configs for the hub mentioned in Part 2 are appropriately set. 

  * To activate the full selenium grid, edit **$NUTCH_HOME/runtime/local/bin/crawl** script:
    - numThreads = maxSession on nodes * num of nodes


## Part 2: Installing plugin for Nutch (where NUTCH_HOME is the root of your nutch install)

 * Ensure that the plugin will be used as the protocol parser in your config

```
<!-- NUTCH_HOME/conf/nutch-site.xml -->

<configuration>
  ...
  <property>
    <name>plugin.includes</name>
    <value>protocol-selenium|urlfilter-regex|parse-(html|tika)|index-(basic|anchor)|urlnormalizer-(pass|regex|basic)|scoring-opic</value>
    <description>Regular expression naming plugin directory names to
    include.  Any plugin not matching this expression is excluded.
    In any case you need at least include the nutch-extensionpoints plugin. By
    default Nutch includes crawling just HTML and plain text via HTTP,
    and basic indexing and search plugins. In order to use HTTPS please enable 
    protocol-httpclient, but be aware of possible intermittent problems with the 
    underlying commons-httpclient library.
    </description>
  </property>
```

* Then ensure that you have the correct configuration set within the following configuration options

```
<!-- protocol-selenium plugin properties -->

<property>
  <name>selenium.driver</name>
  <value>firefox</value>
  <description>
    A String value representing the flavour of Selenium 
    WebDriver() to use. Currently the following options
    exist - 'firefox', 'chrome', 'safari', 'opera' and 'remote'.
    If 'remote' is used it is essential to also set correct properties for
    'selenium.hub.port', 'selenium.hub.path', 'selenium.hub.host' and
    'selenium.hub.protocol'.
  </description>
</property>

<property>
  <name>selenium.take.screenshot</name>
  <value>false</value>
  <description>
    Boolean property determining whether the protocol-selenium
    WebDriver should capture a screenshot of the URL. If set to
    true remember to define the 'selenium.screenshot.location' 
    property as this determines the location screenshots should be 
    persisted to on HDFS. If that property is not set, screenshots
    are simply discarded.
  </description>
</property>

<property>
  <name>selenium.screenshot.location</name>
  <value></value>
  <description>
    The location on disk where a URL screenshot should be saved
    to if the 'selenium.take.screenshot' proerty is set to true.
    By default this is null, in this case screenshots held in memory
    are simply discarded.
  </description>
</property>

<property>
  <name>selenium.hub.port</name>
  <value>4444</value>
  <description>Selenium Hub Location connection port</description>
</property>

<property>
  <name>selenium.hub.path</name>
  <value>/wd/hub</value>
  <description>Selenium Hub Location connection path</description>
</property>

<property>
  <name>selenium.hub.host</name>
  <value>localhost</value>
  <description>Selenium Hub Location connection host</description>
</property>

<property>
  <name>selenium.hub.protocol</name>
  <value>http</value>
  <description>Selenium Hub Location connection protocol</description>
</property>

<property>
  <name>selenium.grid.driver</name>
  <value>firefox</value>
  <description>A String value representing the flavour of Selenium 
    WebDriver() used on the selenium grid. Currently the following options
    exist - 'firefox' </description>
</property>

<property>
  <name>selenium.grid.binary</name>
  <value></value>
  <description>A String value representing the path to the browser binary 
    location for each node
 </description>
</property>

<!-- lib-selenium configuration -->
<property>
  <name>libselenium.page.load.delay</name>
  <value>3</value>
  <description>
    The delay in seconds to use when loading a page with lib-selenium. This
    setting is used by protocol-selenium and protocol-interactiveselenium
    since they depending on lib-selenium for fetching.
  </description>
</property>
```
 * If you've selected 'remote' value for the 'selenium.driver' property, ensure that you've configured
 the additional properties based on your [Selenium-Grid installation](http://www.seleniumhq.org/docs/07_selenium_grid.jsp#installation).

 * Compile nutch
```
ant runtime
```

 * Start your web crawl (Ensure that you followed the above steps and have started your xvfb display as shown above)

## Part 3: Common Pitfalls

* Be sure your browser version and selenium version are compatible 
* Be sure to start the Xvfb window then start selenium
* Disconnecting and reconnect nodes after a hub config change has proven useful in our tests. 
* Be sure that each browser session deallocates its webdriver resource independently of any other tests running on other broswers (check out driver.quit() and driver.close()). 

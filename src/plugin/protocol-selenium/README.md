Nutch Selenium
==============

This plugin allows you to fetch Javascript pages using [Selenium](http://www.seleniumhq.org/), while relying on the rest of the awesome Nutch stack!

The underlying code is based on the nutch-htmlunit plugin, which was in turn based on nutch-httpclient.

# IMPORTANT NOTES:

 * A version of this plugin which relies on the Selenium Hub/Node system can be found here: [nutch-selenium-grid-plugin](https://github.com/momer/nutch-selenium-grid-plugin)

# Installation (tested on Ubuntu 14.0x)

## Part 1: Setting up Selenium

 * Ensure that you have Firefox installed
```
# More info about the package @ [launchpad](https://launchpad.net/ubuntu/trusty/+source/firefox)

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
 * Compile nutch
```
ant runtime
```

 * Start your web crawl (Ensure that you followed the above steps and have started your xvfb display as shown above)
```
NUTCH_HOME/runtime/local/bin/crawl [-i|--index] [-D \"key=value\"] <Seed Dir> <Crawl Dir> <Num Rounds>
```



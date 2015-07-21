Nutch Interactive Selenium
==========================

This protocol plugin allows you to fetch and interact with pages using [Selenium](http://www.seleniumhq.org/).

# Dependencies and Configuration

You will need to have [Selenium](http://www.seleniumhq.org/) and a compatible version of Firefox installed to use this plugin.

Set the protocol to be used in your Nutch configuration files.
```
<!-- NUTCH_HOME/conf/nutch-site.xml -->

<configuration>
  ...
  <property>
    <name>plugin.includes</name>
    <value>protocol-interactiveselenium|urlfilter-regex| ... </value>
    <description></description>
  </property>
```

# Custom Handlers

Only basic functionality is included in the DefaultHandler that comes with the plugin. If you want additional functionality you can implement custom handlers by implementing the InteractiveSeleniumHandler interface in the plugin package. Be sure to also update the plugin config to include your new handler.

```
<!-- NUTCH_HOME/conf/nutch-site.xml -->
<property>
  <name>interactiveselenium.handlers</name>
  <value>NewCustomHandler,DefaultHandler</value>
  <description></description>
</property>
```

# Handler Info

Handlers are called in the order that they're specified in the configuration. A "clean" driver is used for each handler so multiple handlers won't interfere with each other. Page content is appended together from each handler and returned for the request.

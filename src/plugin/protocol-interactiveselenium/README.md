<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

Nutch Interactive Selenium
==========================

This protocol plugin allows you to fetch and interact with pages using [Selenium](https://www.selenium.dev/).

# Dependencies and Configuration

You will need to have [Selenium](https://www.selenium.dev/) and a compatible version of Firefox installed to use this plugin.

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

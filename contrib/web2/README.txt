This folder contains an alternative search frontend implementation
with a goal to be modular and extendable. Currently it
is still in very early stage of development and might miss
some functionality from the original ui.

To achieve the primary goal it uses tiles 
http://struts.apache.org/struts-tiles/index.html
to represent blocks of functinality and layout on a search
(and related) pages. Layout is constructed by using following
tag libraries:

struts-logic
struts-tiles
struts-bean

These tiles blocks can be extended or overridden by plugins
implementing org.apache.nutch.webapp.UIExtensionPoint. A
plugin implementation typically contains own tiles-defs.xml
file that contains definitions (extensions or overriders)
it provides, tiles Controller, supporting classes and libraries.

WebApp extension can contain ui logic (in forms of tiles
controllers and pojos, jar libraries), ui markup
(in form of html, jsp), ui resources css, javascript. there's
no support for binary items at the time of writing this.

To compile you need to fist build your nutch (core and plugins)
after that run ant war to generate war.

The nutch plugins are no included in the generated war and you
need to properly configure where your plugins are. This is achieved
by editing the nutch configuration file <NUTCH_HOME>/conf/nutch-site.xml
the configuration parameter you need to edit is named
'plugin.folders'


Todo:

-Provide some samples of ui plugins


Directory contents

/lib
	contains required additional libraties, all Licenced and maintained by ASF.
	struts.jar (version 1.2.9) and libraries required by struts.
	
/res
  contains stylesheets to transform static html pages or page snippets

/src/main/java
	java sources
	
/src/main/resources
	resources to be packaged in .jar, currently the localized resource files
	
/src/main/webapp
  standard webapp folder
	
build.xml
  contains minimal build instructions to create a .war
  
README.txt
	this file
	
	

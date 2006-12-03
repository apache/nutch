This folder contains an alternative search frontend implementation
with a goal to be modular and extendable. Currently it
is still in very early stage of development and might miss
some functionality from the original ui.

To achieve the primary goal it uses tiles 
http://struts.apache.org/struts-tiles/index.html
to represent blocks of functinality and layout on a search
(and related) pages. Layout is constructed by using following
tag libraries:

jstl-c
jstl-fmt
struts-tiles

These tiles blocks can be extended or overridden by plugins
implementing org.apache.nutch.webapp.UIExtensionPoint. A
plugin implementation typically contains own tiles-defs.xml
file that contains definitions (extensions or overriders)
it provides, tiles Controller, supporting classes and libraries.

WebApp extension can contain ui logic (in forms of tiles
controllers and pojos, jar libraries), ui markup
(in form of html, jsp), ui resources css, javascript.

To compile web2 plugins you must issue command
ant compile-plugins

After compiling you must enable plugins, please refer to nutch
documentation

To build deployable .war issue command ant war.

The nutch plugins are not included in the generated war and you
need to properly configure where your plugins are. This is achieved
by editing the nutch configuration file <NUTCH_HOME>/conf/nutch-site.xml
the configuration parameter you need to edit is named
'plugin.folders'

Directory contents

/lib
	contains required additional libraries, all licenced and
	maintained by ASF. files:
	struts.jar (version 1.2.9) and libraries required by struts:
	commons-beanutils.jar
	commons-collections-3.0.jar
	commons-digester.jar
	
	jstl.jar
	standard.jar
	
	
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
	
	
Web ui plugin source directory structure

/lib
	contains libraries required by extension
	
/src/conf
  configuration files for example tiles-defs.xml
  
/src/java
  java source files

/src/test
  java unit test source files

/src/web
  jsp files
  
/src/resources
  any resources that needs to be exposed to public url space
  (images, css, js...)

/build.xml

/plugin.xml


JSP Templating

Referencing jsp files from tiles-defs:

Referencing jsp resources inside nutch war

absolute path, must start with /WEB-INF

<definition name="searchPage" extends=".layout"
  controllerClass="org.apache.nutch.webapp.controller.CachingSearchController">
  <put name="title" value="cached search"/>
  <put name="pageBody" value="/WEB-INF/jsp/search.jsp" />
</definition>

Referencing jsp resources inside plugins

absolute path, must start with /plugin/

<definition name="searchPage" extends=".layout"
  controllerClass="org.apache.nutch.webapp.controller.CachingSearchController">
  <put name="title" value="cached search"/>
  <put name="pageBody" value="/plugin/search.jsp" />
</definition>

Static resources

You can include static resources (images, html, css, js) in
plugins by putting them in src/resources, at runtime those
resources are exposed in urlspace /resources/<plugin-id>/<resource.ext>


Todo:

-Remove table structures from html to allow more flexible
css layouts

-Refine the idea of extendability
Perhaps we could just porovide maximum number of small tiles and
then let end users combine their favourite ui from those, something like:

<?xml version="1.0" encoding="UTF-8"?>
<plugin
   id="my-web-ui"
   name="site xxx web-ui"
   version="1.0.0"
   provider-name="Me">

   <runtime>
      <library name="my-web-ui.jar">
         <export name="*"/>
      </library>
   </runtime>

   <requires>
      <import plugin="nutch-extensionpoints"/>
      <import plugin="web-search-results"/>
      <import plugin="web-search-navigation"/>
      <import plugin="web-search-clustering"/>
      <import plugin="web-search-preferences"/>
      <import plugin="web-query-propose-spelling"/>
      <import plugin="web-more"/>
      <import plugin="web-caching-oscache"/>
      <import plugin="web-search-preferences"/>
   </requires>

    <extension id="org.apache.nutch.webapp.extension.UI"
      name="My Web UI"
      point="org.apache.nutch.webapp.extension.UI">
      <implementation id="my-ui"
                      class="org.apache.nutch.webapp.extension.UI.VoidImplementation"/>
   </extension>
</plugin>

+ provide their own template and css



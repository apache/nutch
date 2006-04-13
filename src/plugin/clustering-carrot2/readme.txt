This plugin extension adds search results clustering capability to Nutch search 
frontend.

The user interface in Nutch is very limited and you'll most likely need something 
more application-specific. Look at http://www.carrot2.org or 
http://carrot.cs.put.poznan.pl for inspiration.

Libraries in this release are precompiled with stemming and stop words for various
languages present in Carrot2 codebase (imported from the Snowball project). You 
must define the default language and supported languages in Nutch configuration
file (nutch-site.xml). If nothing is given in Nutch configuration, English is 
taken by default.

<!-- Carrot2 Clustering plugin configuration -->

<property>
  <name>extension.clustering.carrot2.defaultLanguage</name>
  <value>en</value>
  <description>Two-letter ISO code of the language. 
  http://www.ics.uci.edu/pub/ietf/http/related/iso639.txt</description>
</property>

<property>
  <name>extension.clustering.carrot2.languages</name>
  <value>en,nl,da,fi,fr,de,it,no,pl,pt,ru,es,sv</value>
  <description>All languages to be used by the clustering plugin. 
  This list includes all currently supported languages (although not all of them
  will successfully instantiate -- support for Polish requires additional
  libraries for instance). Adjust to your needs, fewer languages take less
  memory.
  
  If you use the language recognizer plugin, then each hit will come with its
  own ISO language code. All hits with no explicit language take the default
  language specified in "extension.clustering.carrot2.defaultLanguage" property.
  </description>
</property>


If you need a different language/ clustering algorithm, you'll need to modify 
Nutch plugin code a bit (we don't want the plugin to outgrow Nutch, so we 
include just the essentials here). Ask on carrot@ developers mailing list for 
help if you need it.

Carrot2 JARs come from codebase in version: 1.0.2

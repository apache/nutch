20041129, John Xing

Ontology plugin is a contribution from Michael J Pan <mjpan@cs.ucla.edu>.
Currently it is used to do one kind of query refinement as implemented
in refine-query-init.jsp and refine-query.jsp (both are called by search.jsp).

By default, ontology plugin is compiled, but query refinement based on it
is ignored in search.jsp. To enable query refinement, do the following

(1) specify url(s) of owl files to property extension.ontology.urls in
./conf/nutch-default.xml (or better, ./conf/nutch-site.xml).
(2) uncomment refine-query-init.jsp and refine-query.jsp in search.jsp

If you want to check ontology defined by different owl file, modify property
extension.ontology.urls in ./conf/nutch-default.xml (or better,
./conf/nutch-site.xml), and insert the following to ./bin/nutch:

elif [ "$COMMAND" = "ontology" ] ; then
  for f in $NUTCH_HOME/build/plugins/ontology/*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
  done
  CLASS='org.apache.nutch.ontology.OntologyImpl'

---------------
Possible issue:
---------------
If search.jsp fails with this or similar error:

......
root cause 

java.lang.NoSuchFieldError: actualValueType
        at
com.hp.hpl.jena.datatypes.xsd.XSDDatatype.convertValidatedDataValue(XSDDatatype.java:371)
......

it is because jena and tomcat are using conflicting versions of the same
xerces library. To solve this, one needs to update tomcat's xerces library.
Here's a reference
http://jena.sourceforge.net/jena-faq.html#general-1

<%!

// 20041129, Mike Pan and John Xing
// Initiates Ontology ontology and loads in all owl files.
// Any problem (e.g., missing owl file or exception) will have this plugin
// siliently ignored.
// Please check ./refine-query.jsp, which provides query-refinement hypertext.

private static org.apache.nutch.ontology.Ontology ontology;

// note: should we ignore plugin exceptions, or rethrow it below?
// Rethrowing it effectively prevents the servlet class from
// being loaded into the JVM. Need improvement in future.

static {
  try {
    String urls = org.apache.nutch.util.NutchConf.get("extension.ontology.urls");
    ontology = org.apache.nutch.ontology.OntologyFactory.getOntology();
    if (urls==null || urls.trim().equals("")) {
      // ignored siliently
    } else {
      ontology.load(urls.split("\\s+"));
    }
  } catch (Exception e) {
    // ignored siliently 
  }
};

%>

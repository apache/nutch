<%

// 20041129, Mike Pan and John Xing
// Initiates Ontology ontology and loads in all owl files.
// Any problem (e.g., missing owl file or exception) will have this plugin
// siliently ignored.
// Please check ./refine-query.jsp, which provides query-refinement hypertext.

org.apache.nutch.ontology.Ontology ontology = null;

// note: should we ignore plugin exceptions, or rethrow it below?
// Rethrowing it effectively prevents the servlet class from
// being loaded into the JVM. Need improvement in future.

  try {
    org.apache.nutch.util.NutchConf nutchConf = (org.apache.nutch.util.NutchConf) application.getAttribute(org.apache.nutch.util.NutchConf.class.getName());
  	if (nutchConf == null) {
    	  nutchConf = new org.apache.nutch.util.NutchConf();
    	  application.setAttribute(org.apache.nutch.util.NutchConf.class.getName(), nutchConf);
  	}
    String urls = nutchConf.get("extension.ontology.urls");
    ontology = new org.apache.nutch.ontology.OntologyFactory(nutchConf).getOntology();
    if (urls==null || urls.trim().equals("")) {
      // ignored siliently
    } else {
      ontology.load(urls.split("\\s+"));
    }
  } catch (Exception e) {
    // ignored siliently 
  }

%>

<%--
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
--%>
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
    Configuration nutchConf = NutchConfiguration.get(application);
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

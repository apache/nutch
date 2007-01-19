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
// Displays query-refinement hypertext based on ontology.
// Try to be simple here. No mixing with other features such as clustering.
// Please check refine-query-init.jsp, which does necessary initialization.

List refineList = new ArrayList();

if (ontology != null) {
  Iterator iter = ontology.subclasses(queryString);
  while (iter.hasNext()) {
    refineList.add((String)iter.next());
  }
}

bean.LOG.info("Outputting refine query list");

if (refineList.size() > 0) {
%>
<div>
Refine your search:
<%
  for (int i=0; i<refineList.size(); i++) {
    String searchTerm = (String) refineList.get(i);
    String searchTermHTML = org.apache.nutch.html.Entities.encode(searchTerm);
    String searchQuery = "query="+searchTermHTML;
    String searchURL = "/search.jsp?"+ searchQuery;
%>
<a href="<%=searchURL%>"><%=searchTerm%></a> |
<%
  }
%>
</div><br>
<%
}
%>

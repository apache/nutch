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

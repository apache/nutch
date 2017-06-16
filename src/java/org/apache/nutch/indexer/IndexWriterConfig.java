package org.apache.nutch.indexer;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.*;

public class IndexWriterConfig {

  private String id;

  private String clazz;

  private Map<String, String> params;

  private Map<MappingReader.Actions, Map<String, List<String>>> mapping;

  private IndexWriterConfig(String id, String clazz, Map<String, String> params, Map<MappingReader.Actions, Map<String, List<String>>> mapping) {
    this.id = id;
    this.clazz = clazz;
    this.params = params;
    this.mapping = mapping;
  }

  static IndexWriterConfig getInstanceFromElement(Element rootElement) {
    String id = rootElement.getAttribute("id");
    String clazz = rootElement.getAttribute("class");

    NodeList parametersList = rootElement.getElementsByTagName("param");
    Map<String, String> parameters = new HashMap<>();

    for (int i = 0; i < parametersList.getLength(); i++) {
      Element parameterNode = (Element) parametersList.item(i);
      parameters.put(parameterNode.getAttribute("name"), parameterNode.getAttribute("value"));
    }

    return new IndexWriterConfig(id, clazz, parameters,
            MappingReader.parseMapping((Element) rootElement.getElementsByTagName("mapping").item(0)));
  }

  String getId() {
    return id;
  }

  String getClazz() {
    return clazz;
  }

  Map<String, String> getParams() {
    return params;
  }

  Map<MappingReader.Actions, Map<String, List<String>>> getMapping() {
    return mapping;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ID: ");
    sb.append(id);
    sb.append("\n");

    sb.append("Class: ");
    sb.append(clazz);
    sb.append("\n");

    sb.append("Params {\n");
    for (Map.Entry<String, String> entry : params.entrySet()) {
      sb.append("\t");
      sb.append(entry.getKey());
      sb.append(":\t");
      sb.append(entry.getValue());
      sb.append("\n");
    }
    sb.append("}\n");

    sb.append("Mapping {\n");
    for (Map.Entry<MappingReader.Actions, Map<String, List<String>>> entry : mapping.entrySet()) {
      sb.append("\t");
      sb.append(entry.getKey());
      sb.append(" {\n");
      for (Map.Entry<String, List<String>> entry1 : entry.getValue().entrySet()) {
        sb.append("\t\t");
        sb.append(entry1.getKey());
        sb.append(":\t");
        sb.append(String.join(",", entry1.getValue()));
        sb.append("\n");
      }
      sb.append("\t}\n");
    }
    sb.append("}\n");
    return sb.toString();
  }
}

package org.apache.nutch.searcher.response.xml;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.html.Entities;
import org.apache.nutch.searcher.Hit;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.Summary;
import org.apache.nutch.searcher.response.ResponseWriter;
import org.apache.nutch.searcher.response.SearchResults;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * A ResponseWriter implementation that returns search results in XML format.
 */
public class XMLResponseWriter
  implements ResponseWriter {

  private String contentType = null;
  private Configuration conf;
  private int maxAgeInSeconds;
  private boolean prettyPrint;

  /**
   * Creates and returns a new node within the XML document.
   * 
   * @param doc The XML document.
   * @param parent The parent Node.
   * @param name The name of the new node.
   * 
   * @return The newly created node Element.
   */
  private static Element addNode(Document doc, Node parent, String name) {
    Element child = doc.createElement(name);
    parent.appendChild(child);
    return child;
  }

  /**
   * Creates and returns a new node within the XML document.  The node contains
   * the text supplied as a child node.
   * 
   * @param doc The XML document.
   * @param parent The parent Node.
   * @param name The name of the new node.
   * @param text A text string to append as a child node.
   * 
   * @return The newly created node Element.
   */
  private static void addNode(Document doc, Node parent, String name,
    String text) {
    Element child = doc.createElement(name);
    child.appendChild(doc.createTextNode(getLegalXml(text)));
    parent.appendChild(child);
  }

  /**
   * Adds an attribute name and value to a node Element in the XML document.
   * 
   * @param doc The XML document.
   * @param node The node Element on which to attach the attribute.
   * @param name The name of the attribute.
   * @param value The value of the attribute.
   */
  private static void addAttribute(Document doc, Element node, String name,
    String value) {
    Attr attribute = doc.createAttribute(name);
    attribute.setValue(getLegalXml(value));
    node.getAttributes().setNamedItem(attribute);
  }

  /**
   * Transforms and returns the text string as legal XML text.
   * 
   * @param text The text to transform.
   * 
   * @return The text string in the form of legal XML text.
   */
  protected static String getLegalXml(String text) {
    
    if (text == null) {
      return null;
    }
    StringBuffer buffer = null;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (!isLegalXml(c)) {
        if (buffer == null) {
          buffer = new StringBuffer(text.length());
          buffer.append(text.substring(0, i));
        }
      }
      else {
        if (buffer != null) {
          buffer.append(c);
        }
      }
    }
    return (buffer != null) ? buffer.toString() : text;
  }

  /**
   * Determines if the character is a legal XML character.
   * 
   * @param c The character to check.
   * 
   * @return True if the character is legal xml, false otherwise.
   */
  private static boolean isLegalXml(final char c) {
    return c == 0x9 || c == 0xa || c == 0xd || (c >= 0x20 && c <= 0xd7ff)
      || (c >= 0xe000 && c <= 0xfffd) || (c >= 0x10000 && c <= 0x10ffff);
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.maxAgeInSeconds = conf.getInt("searcher.response.maxage", 86400);
    this.prettyPrint = conf.getBoolean("searcher.response.prettyprint", true);
  }

  public void writeResponse(SearchResults results, HttpServletRequest request,
    HttpServletResponse response)
    throws IOException {

    try {
      
      // create the xml document and add the results and search nodes
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      Document xmldoc = factory.newDocumentBuilder().newDocument();
      Element resEl = addNode(xmldoc, xmldoc, "results");
      Element searchEl = addNode(xmldoc, resEl, "search");
      
      // add common nodes
      String query = results.getQuery();
      addNode(xmldoc, searchEl, "query", query);
      addNode(xmldoc, searchEl, "totalhits",
        String.valueOf(results.getTotalHits()));
      String lang = results.getLang();
      if (lang != null) {
        addNode(xmldoc, searchEl, "lang", lang);
      }
      String sort = results.getSort();
      if (sort != null) {
        addNode(xmldoc, searchEl, "sort", sort);
      }
      addNode(xmldoc, searchEl, "reverse", results.isReverse() ? "true"
        : "false");
      addNode(xmldoc, searchEl, "start", String.valueOf(results.getStart()));
      addNode(xmldoc, searchEl, "end", String.valueOf(results.getEnd()));
      addNode(xmldoc, searchEl, "rows", String.valueOf(results.getRows()));
      addNode(xmldoc, searchEl, "totalhits",
        String.valueOf(results.getTotalHits()));
      addNode(xmldoc, searchEl, "withSummary",
        String.valueOf(results.isWithSummary()));

      String[] searchFields = results.getFields();
      Set<String> fieldSet = new HashSet<String>();
      if (searchFields != null && searchFields.length > 0) {
        addNode(xmldoc, searchEl, "fields", StringUtils.join(searchFields, ","));
        for (int i = 0; i < searchFields.length; i++) {
          fieldSet.add(searchFields[i]);
        }
      }

      // add documents
      Element documents = addNode(xmldoc, resEl, "documents");
      HitDetails[] details = results.getDetails();
      Hit[] hits = results.getHits();
      Summary[] summaries = results.getSummaries();
      for (int i = 0; i < details.length; i++) {

        // every document has an indexno and an indexdocno
        Element document = addNode(xmldoc, documents, "document");
        addAttribute(xmldoc, document, "indexno",
          String.valueOf(hits[i].getIndexNo()));
        addAttribute(xmldoc, document, "indexkey",
          String.valueOf(hits[i].getUniqueKey()));
        
        // don't add summaries not including summaries
        if (summaries != null && results.isWithSummary()) {
          String encSumm = Entities.encode(summaries[i].toString());
          addNode(xmldoc, document, "summary", encSumm);
        }

        // add the fields from hit details
        Element fields = addNode(xmldoc, document, "fields");
        HitDetails detail = details[i];
        for (int j = 0; j < detail.getLength(); j++) {
          String fieldName = detail.getField(j);
          String[] fieldValues = detail.getValues(fieldName);
          
          // if we specified fields to return, only return those fields
          if (fieldSet.size() == 0 || fieldSet.contains(fieldName)) {
            Element field = addNode(xmldoc, fields, "field");
            addAttribute(xmldoc, field, "name", fieldName);
            for (int k = 0; k < fieldValues.length; k++) {
              String encFieldVal = Entities.encode(fieldValues[k]);
              addNode(xmldoc, field, "value", encFieldVal);
            }
          }
        }
      }

      // get the xml source and a transformer to print it out
      DOMSource source = new DOMSource(xmldoc);
      TransformerFactory transFactory = TransformerFactory.newInstance();
      Transformer transformer = transFactory.newTransformer();
      
      // pretty printing can be set through configuration
      if (prettyPrint) {
        transformer.setOutputProperty("indent", "yes");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(
          "{http://xml.apache.org/xslt}indent-amount", "2");
      }
      
      // write out the content to a byte array
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      StreamResult result = new StreamResult(baos);
      transformer.transform(source, result);
      baos.flush();
      baos.close();

      // cache control headers
      SimpleDateFormat sdf = new SimpleDateFormat(
        "E, d MMM yyyy HH:mm:ss 'GMT'");
      long relExpiresInMillis = System.currentTimeMillis()
        + (1000 * maxAgeInSeconds);
      response.setContentType(contentType);
      response.setHeader("Cache-Control", "max-age=" + maxAgeInSeconds);
      response.setHeader("Expires", sdf.format(relExpiresInMillis));
      
      // write out the content to the response
      response.getOutputStream().write(baos.toByteArray());
      response.flushBuffer();
    }
    catch (Exception e) {
      throw new IOException(e);
    }

  }
}

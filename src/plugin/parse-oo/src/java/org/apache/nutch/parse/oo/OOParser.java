/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.parse.oo;

import java.io.*;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.*;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.jaxen.*;
import org.jaxen.jdom.JDOMXPath;
import org.jdom.*;
import org.jdom.input.*;

/**
 * Parser for OpenOffice and OpenDocument formats. This should handle
 * the following formats: Text, Spreadsheet, Presentation, and
 * corresponding templates and "master" documents.
 * 
 * @author Andrzej Bialecki
 */
public class OOParser implements Parser {
  public static final Log LOG = LogFactory.getLog(OOParser.class);
  
  private Configuration conf;

  public OOParser () {
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  public Configuration getConf() {
    return conf;
  }
  
  public ParseResult getParse(Content content) {
    String text = null;
    String title = null;
    Metadata metadata = new Metadata();
    ArrayList outlinks = new ArrayList();

    try {
      byte[] raw = content.getContent();
      String contentLength = content.getMetadata().get("Content-Length");
      if (contentLength != null
            && raw.length != Integer.parseInt(contentLength)) {
          return new ParseStatus(ParseStatus.FAILED, ParseStatus.FAILED_TRUNCATED,
                  "Content truncated at "+raw.length
            +" bytes. Parser can't handle incomplete files.").getEmptyParseResult(content.getUrl(), conf);
      }
      ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(raw));
      ZipEntry ze = null;
      while ((ze = zis.getNextEntry()) != null) {
        if (ze.getName().equals("content.xml")) {
          text = parseContent(ze, zis, outlinks);
        } else if (ze.getName().equals("meta.xml")) {
          parseMeta(ze, zis, metadata);
        }
      }
      zis.close();
    } catch (Exception e) { // run time exception
      e.printStackTrace(LogUtil.getWarnStream(LOG));
      return new ParseStatus(ParseStatus.FAILED,
              "Can't be handled as OO document. " + e).getEmptyParseResult(content.getUrl(), conf);
    }

    title = metadata.get(Metadata.TITLE);
    if (text == null)
      text = "";

    if (title == null)
      title = "";

    Outlink[] links = (Outlink[])outlinks.toArray(new Outlink[outlinks.size()]);
    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, title, links, metadata);
    return ParseResult.createParseResult(content.getUrl(), new ParseImpl(text, parseData));
  }
  
  // extract as much plain text as possible.
  private String parseContent(ZipEntry ze, ZipInputStream zis, ArrayList outlinks) throws Exception {
    StringBuffer res = new StringBuffer();
    FilterInputStream fis = new FilterInputStream(zis) {
      public void close() {};
    };
    SAXBuilder builder = new SAXBuilder();
    Document doc = builder.build(fis);
    Element root = doc.getRootElement();
    // XXX this is expensive for very large documents. In those cases another
    // XXX method (direct processing of SAX events, or XMLPull) should be used.
    XPath path = new JDOMXPath("//text:span | //text:p | //text:tab | //text:tab-stop | //text:a");
    path.addNamespace("text", root.getNamespace("text").getURI());
    Namespace xlink = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");
    List list = path.selectNodes(doc);
    boolean lastp = true;
    for (int i = 0; i < list.size(); i++) {
      Element el = (Element)list.get(i);
      String text = el.getText();
      if (el.getName().equals("p")) {
        // skip empty paragraphs
        if (!text.equals("")) {
          if (!lastp) res.append("\n");
          res.append(text + "\n");
          lastp = true;
        }
      } else if (el.getName().startsWith("tab")) {
        res.append("\t");
        lastp = false;
      } else if (el.getName().equals("a")) {
        List nl = el.getChildren();
        String a = null;
        for (int k = 0; k < nl.size(); k++) {
          Element anchor = (Element)nl.get(k);
          String nsName = anchor.getNamespacePrefix() + ":" + anchor.getName();
          if (!nsName.equals("text:span")) continue;
          a = anchor.getText();
          break;
        }
        String u = el.getAttributeValue("href", xlink);
        if (u == null) u = a; // often anchors are URLs
        try {
          Outlink o = new Outlink(u, a);
          outlinks.add(o);
        } catch (MalformedURLException mue) {
          // skip
        }
        if (a != null && !a.equals("")) {
          if (!lastp) res.append(' ');
          res.append(a);
          lastp = false;
        }
      } else {
        if (!text.equals("")) {
          if (!lastp) res.append(' ');
          res.append(text);
        }
        lastp = false;
      }
    }
    return res.toString();
  }
  
  // extract metadata and convert them to Nutch format
  private void parseMeta(ZipEntry ze, ZipInputStream zis, Metadata metadata) throws Exception {
    FilterInputStream fis = new FilterInputStream(zis) {
      public void close() {};
    };
    SAXBuilder builder = new SAXBuilder();
    Document doc = builder.build(fis);
    XPath path = new JDOMXPath("/office:document-meta/office:meta/*");
    Element root = doc.getRootElement();
    path.addNamespace("office", root.getNamespace("office").getURI());
    List list = path.selectNodes(doc);
    for (int i = 0; i < list.size(); i++) {
      Element n = (Element)list.get(i);
      String text = n.getText();
      if (text.trim().equals("")) continue;
      String name = n.getName();
      if (name.equals("title"))
        metadata.add(Metadata.TITLE, text);
      else if (name.equals("language"))
        metadata.add(Metadata.LANGUAGE, text);
      else if (name.equals("creation-date"))
        metadata.add(Metadata.DATE, text);
      else if (name.equals("print-date"))
        metadata.add(Metadata.LAST_PRINTED, text);
      else if (name.equals("generator"))
        metadata.add(Metadata.APPLICATION_NAME, text);
      else if (name.equals("creator"))
        metadata.add(Metadata.CREATOR, text);
    }
  }
  
  public static void main(String[] args) throws Exception {
    OOParser oo = new OOParser();
    Configuration conf = NutchConfiguration.create();
    oo.setConf(conf);
    FileInputStream fis = new FileInputStream(args[0]);
    byte[] bytes = new byte[fis.available()];
    fis.read(bytes);
    fis.close();
    Content c = new Content("local", "local", bytes, "application/vnd.oasis.opendocument.text", new Metadata(), conf);
    Parse p = oo.getParse(c).get(c.getUrl());
    System.out.println(p.getData());
    System.out.println("Text: '" + p.getText() + "'");
    /*
    // create the test output file
    OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream("e:\\ootest.txt"), "UTF-8");
    osw.write(p.getText());
    osw.flush();
    osw.close();
    */
  }
}

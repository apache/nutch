/*
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
package org.apache.nutch.jsoup.extractor.parse;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.jsoup.extractor.core.JsoupDocumentReader;
import org.apache.nutch.jsoup.extractor.core.JsoupDocument;
import org.apache.nutch.jsoup.extractor.core.JsoupDocument.DocumentField;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseFilter;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

public class JsoupHtmlParser implements ParseFilter {

  public static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private JsoupDocumentReader jsoupDocumentReader;
  private Configuration conf;

  @Override
  public Parse filter(String url, WebPage page, Parse parse,
      HTMLMetaTags metaTags, DocumentFragment doc) {

    for (JsoupDocument jsoupDocument : jsoupDocumentReader.getDocuments()) {
      Matcher matcher = jsoupDocument.getUrlPattern().matcher(url);
      if (matcher.matches()) {
        String htmlContent = new String(page.getContent().array(),
            StandardCharsets.UTF_8);
        Document document = Jsoup.parse(htmlContent, url);
        List<DocumentField> documentFieldList = jsoupDocument
            .getDocumentFields();
        for (DocumentField field : documentFieldList) {
          String fieldContent = getTextContent(document, field);
          if (fieldContent != null) {
            page.getMetadata().put(
                new Utf8(field.getName().toLowerCase(Locale.ROOT)),
                ByteBuffer.wrap(fieldContent.getBytes()));
          }
        }
        break;
      }
    }

    return parse;
  }

  private String getTextContent(Document document, DocumentField field) {
    Elements elements = document.select(field.getCssSelector());
    String attributeKey = field.getAttribute();
    StringBuilder sb = new StringBuilder();
    if (elements != null && !elements.isEmpty()) {
      for (Element element : elements) {
        String content = "";
        if (attributeKey != null && !attributeKey.isEmpty()) {
          content = element.attr(attributeKey);
        } else {
          content = element.ownText();
        }
        if(field.getNormalizer() != null) {
          content = field.getNormalizer().normalize(content);
        }
        sb.append(content);
        sb.append("\t");
      }
    }
    if (sb.length() == "\t".length()) {
      return field.getDefaultValue();
    }
    return sb.toString();
  }

  @Override
  public Collection<Field> getFields() {
    return null;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    jsoupDocumentReader = JsoupDocumentReader.getInstance(conf);
  }
}

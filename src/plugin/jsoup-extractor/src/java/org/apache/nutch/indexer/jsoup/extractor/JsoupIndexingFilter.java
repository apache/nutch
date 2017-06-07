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

package org.apache.nutch.indexer.jsoup.extractor;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.regex.Matcher;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.core.jsoup.extractor.JsoupDocumentReader;
import org.apache.nutch.core.jsoup.extractor.JsoupDocument;
import org.apache.nutch.core.jsoup.extractor.JsoupDocument.DocumentField;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsoupIndexingFilter implements IndexingFilter {

  public static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Configuration conf;
  private JsoupDocumentReader jsoupDocumentReader;

  @Override
  public NutchDocument filter(NutchDocument doc, String url, WebPage page)
      throws IndexingException {
    if (doc == null) {
      return doc;
    }
    for (JsoupDocument jsoupDocument : jsoupDocumentReader.getDocuments()) {
      Matcher matcher = jsoupDocument.getUrlPattern().matcher(url);
      if (matcher.matches()) {
        for (DocumentField documentField : jsoupDocument.getDocumentFields()) {
          String fieldName = documentField.getName();
          String fieldValue = Bytes
              .toString(page.getMetadata().get(new Utf8(fieldName)).array());
          String[] values = fieldValue.split("\t");
          for (String eachValue : values) {
            doc.add(fieldName, eachValue);
          }
        }
        return doc;
      }
    }
    return null;
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

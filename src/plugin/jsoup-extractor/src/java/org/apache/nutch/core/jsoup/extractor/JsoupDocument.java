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
package org.apache.nutch.core.jsoup.extractor;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.nutch.core.jsoup.extractor.normalizer.Normalizable;

public class JsoupDocument {
  
  private Pattern urlPattern;
  private List<DocumentField> documentFields = new ArrayList<>();
  
  public JsoupDocument(Pattern urlPattern) {
    this.urlPattern = urlPattern;
  }

  public static class DocumentField {
    private String name;
    private String cssSelector;
    private String attribute;
    private String defaultValue = "";
    private Normalizable normalizer;

    public DocumentField(String name, String cssSelector) {
      this.name = name;
      this.cssSelector = cssSelector;
    }

    public String getName() {
      return name;
    }

    public String getCssSelector() {
      return cssSelector;
    }

    public void setAttribute(String attribute) {
      this.attribute = attribute;
    }
    
    public String getAttribute() {
      return attribute;
    }

    public String getDefaultValue() {
      return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
      this.defaultValue = defaultValue;
    }

    public Normalizable getNormalizer() {
      return normalizer;
    }

    public void setNormalizer(Normalizable normalizer) {
      this.normalizer = normalizer;
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("DocumentField Name: ").append(this.name);
      sb.append("\n\tCSS-selector: ").append(this.cssSelector);
      if(this.attribute != null) {
        sb.append("\n\tAttribute: ").append(this.attribute);
      }
      if(!this.defaultValue.isEmpty()) {
        sb.append("\n\tDefault value: ").append(this.defaultValue);
      }
      if(this.normalizer != null) {
        sb.append("\n\tNormalizer: ").append(this.normalizer.getClass().getName());
      }
      sb.append("\n");
      return sb.toString();
    }
  }
  
  public Pattern getUrlPattern() {
    return urlPattern;
  }

  public void setUrlPattern(Pattern urlPattern) {
    this.urlPattern = urlPattern;
  }
  
  public List<DocumentField> getDocumentFields() {
    return documentFields;
  }

  public void setDocumentFields(List<DocumentField> documentFields) {
    this.documentFields = documentFields;
  }
  
  public void addField(DocumentField documentField) {
    this.documentFields.add(documentField);
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("JsoupDocument urlPattern: ").append(this.urlPattern.toString());
    for(DocumentField eachField: this.documentFields) {
      sb.append("\t\n").append(eachField.toString());
    }
    sb.append("\n");
    return sb.toString();
  }
}

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
package org.apache.nutch.keymatch;

import org.apache.xerces.util.DOMUtil;
import org.w3c.dom.Element;

public class KeyMatch {

  public static final String TAG_TERM = "term";

  public static final String TAG_URL = "url";

  public static final String TAG_TITLE = "title";

  public static final String ATTR_TYPE = "type";

  public static final String TYPES[] = { "keyword", "phrase", "exact" };

  public static final int TYPE_KEYWORD=0;
  public static final int TYPE_PHRASE=1;
  public static final int TYPE_EXACT=2;
  
  static int counter = 0;

  String term;
  String url;
  String title;
  int type;
  int viewCount=0;

  transient String identifier;

  public KeyMatch() {
    // generate unique id
    this.identifier = "m-" + counter++;
  }

  public KeyMatch(String terms, String url, String title, int type) {
    this();
    this.term = terms;
    this.url = url;
    this.title = title;

    if (type > TYPES.length) {
      this.type = 0;
    } else {
      this.type = type;
    }

  }

  /**
   * Initialize object from Element
   * 
   * @param element
   */
  public void initialize(final Element element) {
    try {
      term = DOMUtil.getChildText(
          element.getElementsByTagName(TAG_TERM).item(0)).trim();

      String stype = element.getAttribute(ATTR_TYPE);
      for (int i = 0; i < TYPES.length; i++) {
        if (TYPES[i].equals(stype)) {
          type = i;
        }
      }

      url = DOMUtil.getChildText(element.getElementsByTagName(TAG_URL).item(0))
          .trim();
      title = DOMUtil.getChildText(
          element.getElementsByTagName(TAG_TITLE).item(0)).trim();
    } catch (Exception ex) {
      // ignore
    }
  }

  /**
   * Fill in element with data from this object
   * 
   * @param element
   */
  public void populateElement(final Element element) {
    final Element term = element.getOwnerDocument().createElement(TAG_TERM);
    term.setNodeValue(this.term);
    element.appendChild(term);
    element.setAttribute(ATTR_TYPE, TYPES[type]);
    final Element url = element.getOwnerDocument().createElement(TAG_URL);
    url.setNodeValue(this.url);
    element.appendChild(url);
    final Element title = element.getOwnerDocument().createElement(TAG_TITLE);
    title.setNodeValue(this.title);
    element.appendChild(title);
  }

  /**
   * @return Returns the term.
   */
  public String getTerm() {
    return term;
  }

  /**
   * @param term
   *          The term to set.
   */
  public void setTerm(final String term) {
    this.term = term;
  }

  /**
   * @return Returns the title.
   */
  public String getTitle() {
    return title;
  }

  /**
   * @param title
   *          The title to set.
   */
  public void setTitle(final String title) {
    this.title = title;
  }

  /**
   * @return Returns the url.
   */
  public String getUrl() {
    return url;
  }

  /**
   * @param url
   *          The url to set.
   */
  public void setUrl(final String url) {
    this.url = url;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object obj) {
    if(obj instanceof KeyMatch) {
      KeyMatch other=(KeyMatch) obj;
      return (other.type==type && other.term.equals(term) && other.title.equals(title) && other.url.equals(url));
    } else 
      return super.equals(obj);
  }

  /**
   * @return Returns the type.
   */
  public int getType() {
    return type;
  }

  /**
   * @param type The type to set.
   */
  public void setType(int type) {
    this.type = type;
  }
}

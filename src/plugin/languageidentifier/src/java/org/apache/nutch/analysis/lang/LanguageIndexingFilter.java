/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.analysis.lang;


// Nutch imports
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.fetcher.FetcherOutput;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.io.UTF8;
import org.apache.nutch.parse.Parse;

// Lucene imports
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Document;


/**
 * An {@link org.apache.nutch.indexer.IndexingFilter} that 
 * add a <code>lang</code> (language) field to the document.
 *
 * It tries to find the language of the document by:
 * <ul>
 *   <li>First, checking if {@link HTMLLanguageParser} add some language
 *       information</li>
 *   <li>Then, checking if a <code>Content-Language</code> HTTP header can be
 *       found</li>
 *   <li>Finaly by analyzing the document content</li>
 * </ul>
 *   
 * @author Sami Siren
 * @author Jerome Charron
 */
public class LanguageIndexingFilter implements IndexingFilter {
  

  /**
   * Constructs a new Language Indexing Filter.
   */
  public LanguageIndexingFilter() {

  }

  // Inherited JavaDoc
  public Document filter(Document doc, Parse parse, UTF8 url, CrawlDatum datum, Inlinks inlinks)
    throws IndexingException {

    //check if X-meta-lang found, possibly put there by HTMLLanguageParser
    String lang = parse.getData().get(HTMLLanguageParser.META_LANG_NAME);

    //check if HTTP-header tels us the language
    if (lang == null) {
        lang = parse.getData().get("Content-Language");
    }
    
    if (lang == null) {
      StringBuffer text = new StringBuffer();
      /*
       * String[] anchors = fo.getAnchors(); for (int i = 0; i < anchors.length;
       * i++) { text+=anchors[i] + " "; }
       */
      text.append(parse.getData().getTitle())
          .append(" ")
          .append(parse.getText());
      lang = LanguageIdentifier.getInstance().identify(text);
    }

    if (lang == null) {
      lang = "unknown";
    }

    doc.add(Field.Keyword("lang", lang));

    return doc;
  }

}

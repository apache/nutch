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

package org.apache.nutch.indexer.basic;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.nutch.parse.Parse;

import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;

import org.apache.nutch.fetcher.FetcherOutput;
import org.apache.nutch.pagedb.FetchListEntry;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Logger;
import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.util.NutchConf;

/** Adds basic searchable fields to a document. */
public class BasicIndexingFilter implements IndexingFilter {
  public static final Logger LOG
    = LogFormatter.getLogger(BasicIndexingFilter.class.getName());

  private static final int MAX_TITLE_LENGTH =
    NutchConf.get().getInt("indexer.max.title.length", 100);

  public Document filter(Document doc, Parse parse, FetcherOutput fo)
    throws IndexingException {
    
    String url = fo.getUrl().toString();
    String host = null;
    try {
      URL u = new URL(url);
      host = u.getHost();
    } catch (MalformedURLException e) {
      throw new IndexingException(e);
    }

    if (host != null) {
      // add host as un-stored, indexed and tokenized
      doc.add(Field.UnStored("host", host));
      // add site as un-stored, indexed and un-tokenized
      doc.add(new Field("site", host, false, true, false));
    }


    // url is both stored and indexed, so it's both searchable and returned
    doc.add(Field.Text("url", url));
    
    // content is indexed, so that it's searchable, but not stored in index
    doc.add(Field.UnStored("content", parse.getText()));
    
    // anchors are indexed, so they're searchable, but not stored in index
    String[] anchors = fo.getAnchors();
    for (int i = 0; i < anchors.length; i++) {
      doc.add(Field.UnStored("anchor", anchors[i]));
    }

    // title
    String title = parse.getData().getTitle();
    if (title.length() > MAX_TITLE_LENGTH) {      // truncate title if needed
      title = title.substring(0, MAX_TITLE_LENGTH);
    }
    // add title indexed and stored so that it can be displayed
    doc.add(Field.Text("title", title));

    return doc;
  }

}

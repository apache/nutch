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

package org.apache.nutch.searcher.site;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.nutch.parse.Parse;

import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;

import org.apache.nutch.fetcher.FetcherOutput;
import org.apache.nutch.pagedb.FetchListEntry;

import java.util.logging.Logger;
import org.apache.nutch.util.LogFormatter;

import java.net.URL;
import java.net.MalformedURLException;

/** Adds the host name to a "site" field, so that it can be searched by
 * SiteQueryFilter. */
public class SiteIndexingFilter implements IndexingFilter {
  public static final Logger LOG
    = LogFormatter.getLogger(SiteIndexingFilter.class.getName());

  public Document filter(Document doc, Parse parse, FetcherOutput fo)
    throws IndexingException {
    
    // parse the url to get the host name
    URL url;                                      
    try {
      url = new URL(fo.getUrl().toString());
    } catch (MalformedURLException e) {
      throw new IndexingException(e);
    }

    // add host as un-stored, indexed and un-tokenized
    doc.add(new Field("site", url.getHost(), false, true, false));

    // return the modified document
    return doc;
  }

}

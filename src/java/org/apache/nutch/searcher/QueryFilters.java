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

package org.apache.nutch.searcher;

import org.apache.nutch.plugin.*;
import org.apache.nutch.searcher.Query.Clause;
import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.util.NutchConf;

import java.util.logging.Logger;
import java.util.*;

import org.apache.lucene.search.BooleanQuery;

/** Creates and caches {@link QueryFilter} implementing plugins.  QueryFilter
 * implementations should define either the "fields" or "raw-fields" attributes
 * for any fields that they process, otherwise these will be ignored by the
 * query parser.  Raw fields are parsed as a single Query.Term, including
 * internal punctuation, while non-raw fields are parsed containing punctuation
 * are parsed as multi-token Query.Phrase's.
 */
public class QueryFilters {
  private static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.searcher.QueryFilters");

  private QueryFilter[] queryFilters;
  private HashSet FIELD_NAMES ;
  private HashSet RAW_FIELD_NAMES;

  private static ArrayList parseFieldNames(Extension extension,
                                           String attribute) {
    String fields = extension.getAttribute(attribute);
    if (fields == null) fields = "";
    return Collections.list(new StringTokenizer(fields, " ,\t\n\r"));
  }

  public QueryFilters(NutchConf nutchConf) {
    this.queryFilters = (QueryFilter[]) nutchConf.getObject(QueryFilter.class
        .getName());
    if (this.queryFilters == null) {
      try {
        ExtensionPoint point = nutchConf.getPluginRepository()
            .getExtensionPoint(QueryFilter.X_POINT_ID);
        if (point == null)
          throw new RuntimeException(QueryFilter.X_POINT_ID + " not found.");
        Extension[] extensions = point.getExtensions();
        QueryFilter[] filters = new QueryFilter[extensions.length];
        for (int i = 0; i < extensions.length; i++) {
          Extension extension = extensions[i];
          ArrayList fieldNames = parseFieldNames(extension, "fields");
          ArrayList rawFieldNames = parseFieldNames(extension, "raw-fields");
          if (fieldNames.size() == 0 && rawFieldNames.size() == 0) {
            LOG.warning("QueryFilter: " + extension.getId()
                + " names no fields.");
            continue;
          }
          filters[i] = (QueryFilter) extension.getExtensionInstance();
          FIELD_NAMES = new HashSet();
          FIELD_NAMES.addAll(fieldNames);
          FIELD_NAMES.addAll(rawFieldNames);
          nutchConf.setObject("FIELD_NAMES", FIELD_NAMES);
          RAW_FIELD_NAMES = new HashSet();
          RAW_FIELD_NAMES.addAll(rawFieldNames);
          nutchConf.setObject("RAW_FIELD_NAMES", RAW_FIELD_NAMES);
        }
        nutchConf.setObject(QueryFilter.class.getName(), filters);
      } catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      }
      this.queryFilters = (QueryFilter[]) nutchConf.getObject(QueryFilter.class
          .getName());
    } else {
      // cache already filled
      FIELD_NAMES = (HashSet) nutchConf.getObject("FIELD_NAMES");
      RAW_FIELD_NAMES = (HashSet) nutchConf.getObject("RAW_FIELD_NAMES");
    }
  }              

  /** Run all defined filters. */
  public BooleanQuery filter(Query input) throws QueryException {
    // first check that all field names are claimed by some plugin
    Clause[] clauses = input.getClauses();
    for (int i = 0; i < clauses.length; i++) {
      Clause c = clauses[i];
      if (!isField(c.getField()))
        throw new QueryException("Not a known field name:"+c.getField());
    }

    // then run each plugin
    BooleanQuery output = new BooleanQuery();
    for (int i = 0; i < this.queryFilters.length; i++) {
      output = this.queryFilters[i].filter(input, output);
    }
    return output;
  }

  public boolean isField(String name) {
    return FIELD_NAMES.contains(name);
  }
  
  public boolean isRawField(String name) {
    return RAW_FIELD_NAMES.contains(name);
  }
}

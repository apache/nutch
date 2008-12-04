package org.apache.nutch.searcher.custom;

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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.StringUtils;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.nutch.indexer.field.CustomFields;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.searcher.QueryException;
import org.apache.nutch.searcher.QueryFilter;
import org.apache.nutch.searcher.Query.Clause;

public class CustomFieldQueryFilter
  implements QueryFilter {

  public static final Log LOG = LogFactory.getLog(CustomFields.class);
  private Configuration conf;
  private List<String> fieldNames = new ArrayList<String>();
  private Map<String, Float> boosts = new HashMap<String, Float>();

  public CustomFieldQueryFilter() {
  }

  public void setConf(Configuration conf) {

    try {
      this.conf = conf;
      FileSystem fs = FileSystem.get(conf);
      String configFile = conf.get("custom.fields.config", "custom-fields.xml");
      LOG.info("Reading configuration field configuration from " + configFile);
      Properties customFieldProps = new Properties();
      InputStream fis = CustomFields.class.getClassLoader().getResourceAsStream(
        configFile);
      if (fis == null) {
        throw new IOException("Was unable to open " + configFile);
      }
      customFieldProps.loadFromXML(fis);
      Enumeration keys = customFieldProps.keys();
      while (keys.hasMoreElements()) {
        String prop = (String)keys.nextElement();
        if (prop.endsWith(".name")) {
          String propName = prop.substring(0, prop.length() - 5);
          String name = customFieldProps.getProperty(prop);
          fieldNames.add(name);
          String boostKey = propName + ".boost";
          if (customFieldProps.containsKey(boostKey)) {
            float boost = Float.parseFloat(customFieldProps.getProperty(boostKey));
            boosts.put(name, boost);
          }
        }
      }
    }
    catch (Exception e) {
      LOG.error("Error loading custom field properties:\n"
        + StringUtils.stringifyException(e));
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  public BooleanQuery filter(Query input, BooleanQuery output)
    throws QueryException {

    // examine each clause in the Nutch query
    Clause[] clauses = input.getClauses();
    for (int i = 0; i < clauses.length; i++) {
      Clause c = clauses[i];

      // skip non-matching clauses
      String fieldName = c.getField();
      if (!fieldNames.contains(fieldName)) {
        continue;
      }
      String value = c.getTerm().toString().toLowerCase();

      // add a Lucene TermQuery for this clause
      TermQuery clause = new TermQuery(new Term(fieldName, value));
      // set boost
      if (boosts.containsKey(fieldName)) {
        clause.setBoost(boosts.get(fieldName));
      }

      // add it as specified in query
      output.add(clause, (c.isProhibited() ? BooleanClause.Occur.MUST_NOT
        : (c.isRequired() ? BooleanClause.Occur.MUST
          : BooleanClause.Occur.SHOULD)));
    }

    // return the modified Lucene query
    return output;
  }
}

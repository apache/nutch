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

package org.apache.nutch.searcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.ToStringUtils;
import org.apache.nutch.indexer.solr.SolrWriter;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class SolrSearchBean implements SearchBean {

  public static final Log LOG = LogFactory.getLog(SolrSearchBean.class);

  private final SolrServer solr;

  private final QueryFilters filters;

  public SolrSearchBean(Configuration conf, String solrServer)
  throws IOException {
    solr = new CommonsHttpSolrServer(solrServer);
    filters = new QueryFilters(conf);
  }

  public String getExplanation(Query query, Hit hit) throws IOException {
    return "SOLR backend does not support explanations yet.";
  }

  @SuppressWarnings("unchecked")
  public Hits search(Query query, int numHits, String dedupField,
                     String sortField, boolean reverse)
  throws IOException {

    // filter query string
    final BooleanQuery bQuery = filters.filter(query);

    final SolrQuery solrQuery = new SolrQuery(stringify(bQuery));

    solrQuery.setRows(numHits);

    if (sortField == null) {
      solrQuery.setFields(dedupField, "score", "id");
      sortField = "score";
    } else {
      solrQuery.setFields(dedupField, sortField, "id");
      solrQuery.setSortField(sortField, reverse ? ORDER.asc : ORDER.desc);
    }

    QueryResponse response;
    try {
      response = solr.query(solrQuery);
    } catch (final SolrServerException e) {
      throw SolrWriter.makeIOException(e);
    }

    final SolrDocumentList docList = response.getResults();

    final Hit[] hitArr = new Hit[docList.size()];
    for (int i = 0; i < hitArr.length; i++) {
      final SolrDocument solrDoc = docList.get(i);

      final Object raw = solrDoc.getFirstValue(sortField);
      WritableComparable sortValue;

      if (raw instanceof Integer) {
        sortValue = new IntWritable(((Integer)raw).intValue());
      } else if (raw instanceof Float) {
        sortValue = new FloatWritable(((Float)raw).floatValue());
      } else if (raw instanceof String) {
        sortValue = new Text((String)raw);
      } else if (raw instanceof Long) {
        sortValue = new LongWritable(((Long)raw).longValue());
      } else {
        throw new RuntimeException("Unknown sort value type!");
      }

      final String dedupValue = (String) solrDoc.getFirstValue(dedupField);

      final String uniqueKey = (String )solrDoc.getFirstValue("id");

      hitArr[i] = new Hit(uniqueKey, sortValue, dedupValue);
    }

    return new Hits(docList.getNumFound(), hitArr);
  }

  public HitDetails getDetails(Hit hit) throws IOException {
    QueryResponse response;
    try {
      response = solr.query(new SolrQuery("id:\"" + hit.getUniqueKey() + "\""));
    } catch (final SolrServerException e) {
      throw SolrWriter.makeIOException(e);
    }

    final SolrDocumentList docList = response.getResults();
    if (docList.getNumFound() == 0) {
      return null;
    }

    return buildDetails(docList.get(0));
  }

  public HitDetails[] getDetails(Hit[] hits) throws IOException {
    final StringBuilder buf = new StringBuilder();
    buf.append("(");
    for (final Hit hit : hits) {
      buf.append(" id:\"");
      buf.append(hit.getUniqueKey());
      buf.append("\"");
    }
    buf.append(")");

    QueryResponse response;
    try {
      response = solr.query(new SolrQuery(buf.toString()));
    } catch (final SolrServerException e) {
      throw SolrWriter.makeIOException(e);
    }

    final SolrDocumentList docList = response.getResults();
    if (docList.size() < hits.length) {
      throw new RuntimeException("Missing hit details! Found: " +
                                 docList.size() + ", expecting: " +
                                 hits.length);
    }

    /* Response returned from SOLR server may be out of
     * order. So we make sure that nth element of HitDetails[]
     * is the detail of nth hit.
     */
    final Map<String, HitDetails> detailsMap =
      new HashMap<String, HitDetails>(hits.length);
    for (final SolrDocument solrDoc : docList) {
      final HitDetails details = buildDetails(solrDoc);
      detailsMap.put(details.getValue("id"), details);
    }

    final HitDetails[] detailsArr = new HitDetails[hits.length];
    for (int i = 0; i < hits.length; i++) {
      detailsArr[i] = detailsMap.get(hits[i].getUniqueKey());
    }

    return detailsArr;
  }

  public boolean ping() throws IOException {
    try {
      return solr.ping().getStatus() == 0;
    } catch (final SolrServerException e) {
      throw SolrWriter.makeIOException(e);
    }
  }

  public void close() throws IOException { }

  private static HitDetails buildDetails(SolrDocument solrDoc) {
    final List<String> fieldList = new ArrayList<String>();
    final List<String> valueList = new ArrayList<String>();
    for (final String field : solrDoc.getFieldNames()) {
      for (final Object o : solrDoc.getFieldValues(field)) {
        fieldList.add(field);
        valueList.add(o.toString());
      }
    }

    final String[] fields = fieldList.toArray(new String[fieldList.size()]);
    final String[] values = valueList.toArray(new String[valueList.size()]);
    return new HitDetails(fields, values);
  }

  /* Hackish solution for stringifying queries. Code from BooleanQuery.
   * This is necessary because a BooleanQuery.toString produces
   * statements like feed:http://www.google.com which doesn't work, we
   * need feed:"http://www.google.com".
   */
  private static String stringify(BooleanQuery bQuery) {
    final StringBuilder buffer = new StringBuilder();
    final boolean needParens=(bQuery.getBoost() != 1.0) ||
                       (bQuery.getMinimumNumberShouldMatch()>0) ;
    if (needParens) {
      buffer.append("(");
    }

    final BooleanClause[] clauses  = bQuery.getClauses();
    int i = 0;
    for (final BooleanClause c : clauses) {
      if (c.isProhibited())
        buffer.append("-");
      else if (c.isRequired())
        buffer.append("+");

      final org.apache.lucene.search.Query subQuery = c.getQuery();
      if (subQuery instanceof BooleanQuery) {   // wrap sub-bools in parens
        buffer.append("(");
        buffer.append(c.getQuery().toString(""));
        buffer.append(")");
      } else if (subQuery instanceof TermQuery) {
        final Term term = ((TermQuery) subQuery).getTerm();
        buffer.append(term.field());
        buffer.append(":\"");
        buffer.append(term.text());
        buffer.append("\"");
      } else {
        buffer.append(" ");
        buffer.append(c.getQuery().toString());
      }

      if (i++ != clauses.length - 1) {
        buffer.append(" ");
      }
    }

    if (needParens) {
      buffer.append(")");
    }

    if (bQuery.getMinimumNumberShouldMatch()>0) {
      buffer.append('~');
      buffer.append(bQuery.getMinimumNumberShouldMatch());
    }

    if (bQuery.getBoost() != 1.0f) {
      buffer.append(ToStringUtils.boost(bQuery.getBoost()));
    }

    return buffer.toString();
  }

}

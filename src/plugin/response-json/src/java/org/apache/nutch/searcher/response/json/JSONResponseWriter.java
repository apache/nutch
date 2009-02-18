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
package org.apache.nutch.searcher.response.json;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.searcher.Hit;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.Summary;
import org.apache.nutch.searcher.response.RequestUtils;
import org.apache.nutch.searcher.response.ResponseWriter;
import org.apache.nutch.searcher.response.SearchResults;

/**
 * A ResponseWriter implementation that returns search results in JSON format.
 */
public class JSONResponseWriter
  implements ResponseWriter {

  private String contentType = null;
  private Configuration conf;
  private int maxAgeInSeconds;
  private boolean prettyPrint = true;

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.maxAgeInSeconds = conf.getInt("searcher.response.maxage", 86400);
    this.prettyPrint = conf.getBoolean("searcher.response.prettyprint", true);
  }

  public void writeResponse(SearchResults results, HttpServletRequest request,
    HttpServletResponse response)
    throws IOException {

    // the function name, if any wrapping the JSON output
    String func = RequestUtils.getStringParameter(request, "func");

    // create the JSON object and add common values
    JSONObject jsonObj = new JSONObject();
    jsonObj.accumulate("query", results.getQuery());
    jsonObj.accumulate("lang", results.getLang());
    jsonObj.accumulate("sort", results.getSort());
    jsonObj.accumulate("reverse", results.isReverse());
    jsonObj.accumulate("start", results.getStart());
    jsonObj.accumulate("end", results.getEnd());
    jsonObj.accumulate("rows", results.getRows());
    jsonObj.accumulate("totalhits", results.getTotalHits());
    jsonObj.accumulate("withSummary", results.isWithSummary());

    String[] searchFields = results.getFields();
    Set<String> fieldSet = new HashSet<String>();
    if (searchFields != null && searchFields.length > 0) {
      jsonObj.accumulate("fields", StringUtils.join(searchFields, ","));
      for (int i = 0; i < searchFields.length; i++) {
        fieldSet.add(searchFields[i]);
      }
    }

    // add the documents from search hits
    JSONArray docsAr = new JSONArray();
    HitDetails[] details = results.getDetails();
    Hit[] hits = results.getHits();
    Summary[] summaries = results.getSummaries();
    for (int i = 0; i < details.length; i++) {
      
      // every document has an indexno and an indexdocno
      JSONObject result = new JSONObject();
      HitDetails detail = details[i];
      Hit hit = hits[i];
      result.accumulate("indexno", hit.getIndexNo());
      result.accumulate("indexkey", hit.getUniqueKey());
      
      // don't add summaries not including summaries
      if (summaries != null && results.isWithSummary()) {
        Summary summary = summaries[i];
        result.accumulate("summary", summary.toString());
      }
      
      // add the fields from hit details
      JSONObject fields = new JSONObject();
      for (int k = 0; k < detail.getLength(); k++) {
        String name = detail.getField(k);
        String[] values = detail.getValues(name);
        
        // if we specified fields to return, only return those fields
        if (fieldSet.size() == 0 || fieldSet.contains(name)) {
          JSONArray valuesAr = new JSONArray();
          for (int m = 0; m < values.length; m++) {
            valuesAr.add(values[m]);
          }
          fields.accumulate(name, valuesAr);
        }
      }
      result.accumulate("fields", fields);
      docsAr.add(result);
    }

    jsonObj.accumulate("documents", docsAr);
    
    // pretty printing can be set through configuration, write out the wrapper
    // function if there is one
    StringBuilder builder = new StringBuilder();
    if (StringUtils.isNotBlank(func)) {
      builder.append(func + "(");
    }    
    builder.append(prettyPrint ? jsonObj.toString(2) : jsonObj.toString());
    if (StringUtils.isNotBlank(func)) {
      builder.append(")");
    }

    // Cache control headers
    SimpleDateFormat sdf = new SimpleDateFormat("E, d MMM yyyy HH:mm:ss 'GMT'");
    long relExpiresInMillis = System.currentTimeMillis()
      + (1000 * maxAgeInSeconds);
    response.setContentType(contentType);
    response.setHeader("Cache-Control", "max-age=" + maxAgeInSeconds);
    response.setHeader("Expires", sdf.format(relExpiresInMillis));
    
    // write out the content to the response
    response.getOutputStream().write(builder.toString().getBytes());
    response.flushBuffer();
  }

}

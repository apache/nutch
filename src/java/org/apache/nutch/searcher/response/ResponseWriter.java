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
package org.apache.nutch.searcher.response;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configurable;
import org.apache.nutch.plugin.Pluggable;

/**
 * Nutch extension point which allow writing search results in many different
 * output formats.
 */
public interface ResponseWriter
  extends Pluggable, Configurable {

  public final static String X_POINT_ID = ResponseWriter.class.getName();
  
  /**
   * Sets the returned content MIME type.  Populated though variables set in
   * the plugin.xml file of the ResponseWriter.  This allows easily changing
   * output content types, for example for JSON from text/plain during tesing
   * and debugging to application/json in production.
   * 
   * @param contentType The MIME content type to set.
   */
  public void setContentType(String contentType);

  /**
   * Writes out the search results response to the HttpServletResponse.
   * 
   * @param results The SearchResults object containing hits and other info.
   * @param request The HttpServletRequest object.
   * @param response The HttpServletResponse object.
   * 
   * @throws IOException If an error occurs while writing out the response.
   */
  public void writeResponse(SearchResults results, HttpServletRequest request,
    HttpServletResponse response)
    throws IOException;

}

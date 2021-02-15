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
package org.apache.nutch.tools;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Interface for all CommonCrawl formatter. It provides the signature for the
 * method used to get JSON data.
 *
 * @author gtotaro
 *
 */
public interface CommonCrawlFormat extends Closeable {

  /**
   * Get a string representation of the JSON structure of the URL content.
   * @return the JSON URL content string
   * @throws IOException if there is a fatal I/O error obtaining JSON data
   */
  public String getJsonData() throws IOException;

  /**
   * Returns a string representation of the JSON structure of the URL content.
   * Takes into consideration both the {@link Content} and {@link Metadata}
   *
   * @param url the canonical url
   * @param content url {@link Content}
   * @param metadata url {@link Metadata}
   * @return the JSON URL content string
   * @throws IOException if there is a fatal I/O error obtaining JSON data
   */
  public String getJsonData(String url, Content content, Metadata metadata)
      throws IOException;

  /**
   * Returns a string representation of the JSON structure of the URL content.
   * Takes into consideration the {@link Content}, {@link Metadata} and
   * {@link ParseData}.
   * 
   * @param url the canonical url
   * @param content url {@link Content}
   * @param metadata url {@link Metadata}
   * @param parseData url {@link ParseData}
   * @return the JSON URL content string
   * @throws IOException if there is a fatal I/O error obtaining JSON data
   */
  public String getJsonData(String url, Content content, Metadata metadata,
      ParseData parseData) throws IOException;


  /**
   * sets inlinks of this document
   * @param inLinks list of inlinks
   */
  void setInLinks(List<String> inLinks);


  /**
   * gets set of inlinks
   * @return gets inlinks of this document
   */
  List<String> getInLinks();

  /**
   * Optional method that could be implemented if the actual format needs some
   * close procedure.
   */
  public abstract void close();
}

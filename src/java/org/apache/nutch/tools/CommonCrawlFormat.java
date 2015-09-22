/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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

import java.io.IOException;

/**
 * Interface for all CommonCrawl formatter. It provides the signature for the
 * method used to get JSON data.
 *
 * @author gtotaro
 *
 */
public interface CommonCrawlFormat {

  /**
   *
   * @param mapAll If {@code true} maps all metdata on the JSON structure.
   * @return the JSON data
   */
  //public String getJsonData(boolean mapAll) throws IOException;
  public String getJsonData() throws IOException;

  /**
   * Returns a string representation of the JSON structure of the URL content
   *
   * @param url
   * @param content
   * @param metadata
   * @return
   */
  public String getJsonData(String url, Content content, Metadata metadata)
      throws IOException;

  /**
   * Returns a string representation of the JSON structure of the URL content
   * takes into account the parsed metadata about the URL
   *
   * @param url
   * @param content
   * @param metadata
   * @return
   */
  public String getJsonData(String url, Content content, Metadata metadata,
      ParseData parseData) throws IOException;

  /**
   * Optional method that could be implemented if the actual format needs some
   * close procedure.
   */
  public abstract void close();
}

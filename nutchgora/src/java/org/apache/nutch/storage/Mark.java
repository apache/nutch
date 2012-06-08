/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.storage;

import org.apache.avro.util.Utf8;

public enum Mark {
  INJECT_MARK("_injmrk_"), GENERATE_MARK("_gnmrk_"), FETCH_MARK("_ftcmrk_"),
  PARSE_MARK("__prsmrk__"), UPDATEDB_MARK("_updmrk_"), INDEX_MARK("_idxmrk_");

  private Utf8 name;

  Mark(String name) {
    this.name = new Utf8(name);
  }

  public void putMark(WebPage page, Utf8 markValue) {
    page.putToMarkers(name, markValue);
  }

  public void putMark(WebPage page, String markValue) {
    putMark(page, new Utf8(markValue));
  }

  public Utf8 removeMark(WebPage page) {
    return page.removeFromMarkers(name);
  }

  public Utf8 checkMark(WebPage page) {
    return page.getFromMarkers(name);
  }

  /**
   * Remove the mark only if the mark is present on the page.
   * @param page The page to remove the mark from.
   * @return If the mark was present.
   */
  public Utf8 removeMarkIfExist(WebPage page) {
    if (page.getFromMarkers(name) != null) {
      return page.removeFromMarkers(name);
    }
    return null;
  }
}

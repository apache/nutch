/**
 * ****************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ****************************************************************************
 */
package org.apache.nutch.parse;

import org.apache.nutch.metadata.Metadata;

import java.util.List;
import java.util.Map;

public class NutchSitemapParse {

  private Map<Outlink, Metadata> outlinkMap;
  private org.apache.nutch.storage.ParseStatus parseStatus;

  public NutchSitemapParse() {
  }

  public NutchSitemapParse(Map<Outlink, Metadata> outlinkMap,
      org.apache.nutch.storage.ParseStatus parseStatus) {
    this.outlinkMap = outlinkMap;
    this.parseStatus = parseStatus;
  }

  public Map<Outlink, Metadata> getOutlinkMap() {
    return outlinkMap;
  }

  public org.apache.nutch.storage.ParseStatus getParseStatus() {
    return parseStatus;
  }

  public void setOutlinks(Map<Outlink, Metadata> outlinkMap) {
    this.outlinkMap = outlinkMap;
  }

  public void setParseStatus(org.apache.nutch.storage.ParseStatus parseStatus) {
    this.parseStatus = parseStatus;
  }
}

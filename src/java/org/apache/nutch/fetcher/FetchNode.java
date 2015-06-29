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
package org.apache.nutch.fetcher;

import org.apache.hadoop.io.Text;
import org.apache.nutch.parse.Outlink;

public class FetchNode {
  private Text url = null;
  private Outlink[] outlinks;
  private int status = 0;
  private String title = null;
  private long fetchTime = 0;
  
  public Text getUrl() {
    return url;
  }
  public void setUrl(Text url) {
    this.url = url;
  }
  public Outlink[] getOutlinks() {
    return outlinks;
  }
  public void setOutlinks(Outlink[] links) {
    this.outlinks = links;
  }
  public int getStatus() {
    return status;
  }
  public void setStatus(int status) {
    this.status = status;
  }
  public String getTitle() {
    return title;
  }
  public void setTitle(String title) {
    this.title = title;
  }
  public long getFetchTime() {
    return fetchTime;
  }
  public void setFetchTime(long fetchTime) {
    this.fetchTime = fetchTime;
  }  
}
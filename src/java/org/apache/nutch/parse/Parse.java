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
package org.apache.nutch.parse;


public class Parse {

  private String text;
  private String title;
  private Outlink[] outlinks;
  private org.apache.nutch.storage.ParseStatus parseStatus;

  public Parse() {
  }

  public Parse(String text, String title, Outlink[] outlinks,
      org.apache.nutch.storage.ParseStatus parseStatus) {
    this.text = text;
    this.title = title;
    this.outlinks = outlinks;
    this.parseStatus = parseStatus;
  }

  public String getText() {
    return text;
  }

  public String getTitle() {
    return title;
  }

  public Outlink[] getOutlinks() {
    return outlinks;
  }

  public org.apache.nutch.storage.ParseStatus getParseStatus() {
    return parseStatus;
  }

  public void setText(String text) {
    this.text = text;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public void setOutlinks(Outlink[] outlinks) {
    this.outlinks = outlinks;
  }

  public void setParseStatus(org.apache.nutch.storage.ParseStatus parseStatus) {
    this.parseStatus = parseStatus;
  }
}

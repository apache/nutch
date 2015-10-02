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
package org.apache.nutch.fetcher;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.nutch.parse.Outlink;

/**
 * This class is used to capture the various events occurring 
 * at fetch time. These events are sent to a queue implementing the publisher
 *
 */
@SuppressWarnings("serial")
public class FetcherThreadEvent implements Serializable{

  public static enum PublishEventType {START, END, REPORT}
 
  
  private PublishEventType eventType;
  private Map<String, Object> eventData;
  private String url; 
  private Long timestamp; 
  
  public FetcherThreadEvent(PublishEventType eventType, String url) {
    this.eventType = eventType;
    this.url = url;
    this.timestamp = System.currentTimeMillis();
  }
  
  public PublishEventType getEventType() {
    return eventType;
  }
  public void setEventType(PublishEventType eventType) {
    this.eventType = eventType;
  }
  public Map<String, Object> getEventData() {
    return eventData;
  }
  public void setEventData(Map<String, Object> eventData) {
    this.eventData = eventData;
  }
  public String getUrl() {
    return url;
  }
  public void setUrl(String url) {
    this.url = url;
  }
  
  public void addEventData(String key, Object value) {
    if(eventData == null) {
      eventData = new HashMap<String, Object>();
    }
    eventData.put(key, value);
  }
  
  public void addOutlinksToEventData(Collection<Outlink> links) {
    ArrayList<Map<String, String>> outlinkList = new ArrayList<>();
    for(Outlink link: links) {
      Map<String, String> outlink = new HashMap<>();
      outlink.put("url", link.getToUrl());
      outlink.put("anchor", link.getAnchor());
      outlinkList.add(outlink);
    }
    this.addEventData("outlinks", outlinkList);
  }
  public Long getTimestamp() {
    return timestamp;
  }
  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

}

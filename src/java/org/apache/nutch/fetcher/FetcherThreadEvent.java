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

import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Outlink;

/**
 * This class is used to capture the various events occurring 
 * at fetch time. These events are sent to a queue implementing the publisher
 *
 */
public class FetcherThreadEvent implements Serializable{

  /** Type of event to specify start, end or reporting of a fetch item.  **/
  public static enum PublishEventType {START, END, REPORT}
  
  private PublishEventType eventType;
  private Map<String, Object> eventData;
  private String url; 
  private Long timestamp; 
  
  /**
   * Constructor to create an event to be published
   * @param eventType	Type of {@link #eventType event} being created 
   * @param url	URL of the fetched page to which this event belongs to
   */
  public FetcherThreadEvent(PublishEventType eventType, String url) {
    this.eventType = eventType;
    this.url = url;
    this.timestamp = System.currentTimeMillis();
  }
  
  /**
   * Get type of this event object
   * @return {@link PublishEventType Event} type
   */
  public PublishEventType getEventType() {
    return eventType;
  }
  
  /**
   * Set event type of this object
   * @param eventType	Set {@link #eventType event} type
   */
  public void setEventType(PublishEventType eventType) {
    this.eventType = eventType;
  }
  
  /**
   * Get event data
   * @return
   */
  public Map<String, Object> getEventData() {
    return eventData;
  }
  /** 
   * Set metadata to this even
   * @param eventData	A map containing important information relevant 
   * 					to this event (fetched page).
   * 					Ex - score, title, outlinks, content-type, etc
   */
  public void setEventData(Map<String, Object> eventData) {
    this.eventData = eventData;
  }
  
  /**
   * Get URL of this event
   * @return {@link #url URL} of this event
   */
  public String getUrl() {
    return url;
  }
  
  /**
   * Set URL of this event (fetched page)
   * @param url	URL of the fetched page
   */
  public void setUrl(String url) {
    this.url = url;
  }
  /**
   * Add new data to the eventData object. 
   * @param key	A key to refer to the data being added to this event
   * @param value	Data to be stored in the event referenced by the above key
   */
  public void addEventData(String key, Object value) {
    if(eventData == null) {
      eventData = new HashMap<>();
    }
    eventData.put(key, value);
  }
  
  /**
   * Given a collection of lists this method will add it 
   * the oultink metadata 
   * @param links	A collection of outlinks generating from the fetched page
   * 				this event refers to
   */
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
  
  /**
   * Get timestamp of current event. 
   * @return {@link #timestamp Timestamp}
   */
  public Long getTimestamp() {
    return timestamp;
  }
  
  /**
   * Set timestamp for this event
   * @param timestamp	Timestamp of the occurrence of this event
   */
  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

}

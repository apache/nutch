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
package org.apache.nutch.service.model.response;

import java.util.ArrayList;
import java.util.List;

import org.apache.nutch.parse.Outlink;

public class FetchNodeDbInfo {
  
  private String url;
  private int status;
  private int numOfOutlinks;
  private List<ChildNode> children = new ArrayList<>();
  
  
  public String getUrl() {
    return url;
  }


  public void setUrl(String url) {
    this.url = url;
  }


  public int getStatus() {
    return status;
  }


  public void setStatus(int status) {
    this.status = status;
  }


  public int getNumOfOutlinks() {
    return numOfOutlinks;
  }


  public void setNumOfOutlinks(int numOfOutlinks) {
    this.numOfOutlinks = numOfOutlinks;
  }
  
  public void setChildNodes(Outlink[] links){
    ChildNode childNode;
    for(Outlink outlink: links){
      childNode = new ChildNode(outlink.getToUrl(), outlink.getAnchor());
      children.add(childNode);
    }
  }


  private class ChildNode{
    private String childUrl;
    private String anchorText;
    
    public ChildNode(String childUrl, String anchorText){
      this.childUrl = childUrl;
      this.anchorText = anchorText;
    }
    
    public String getAnchorText() {
      return anchorText;
    }
    public void setAnchorText(String anchorText) {
      this.anchorText = anchorText;
    }
    public String getChildUrl() {
      return childUrl;
    }
    public void setChildUrl(String childUrl) {
      this.childUrl = childUrl;
    }
  }


  public List<ChildNode> getChildren() {
    return children;
  }


  public void setChildren(List<ChildNode> children) {
    this.children = children;
  }
  
}

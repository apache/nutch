package org.apache.nutch.service.model.response;

import java.util.ArrayList;
import java.util.List;

import org.apache.nutch.parse.Outlink;

public class FetchNodeDbInfo {
  
  private String url;
  private int status;
  private int numOfOutlinks;
  private List<ChildNode> children = new ArrayList<ChildNode>();
  
  
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

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
    System.out.println(this.hashCode() + " Setting url to : " + url.toString());
    this.url = url;
  }
  public Outlink[] getOutlinks() {
    return outlinks;
  }
  public void setOutlinks(Outlink[] links) {
    System.out.println(this.hashCode() + " Setting outlinks to : " + links.length);
    this.outlinks = links;
  }
  public int getStatus() {
    return status;
  }
  public void setStatus(int status) {
    System.out.println(this.hashCode() + " Setting status to : " + status);
    this.status = status;
  }
  public String getTitle() {
    return title;
  }
  public void setTitle(String title) {
    System.out.println(this.hashCode() + " Setting title to : " + title);
    this.title = title;
  }
  public long getFetchTime() {
    return fetchTime;
  }
  public void setFetchTime(long fetchTime) {
    System.out.println(this.hashCode() + " Setting fetchTime to : " + fetchTime);
    this.fetchTime = fetchTime;
  }
  
//  public String toString(){
//    StringBuffer sb = new StringBuffer();
//    sb.append("URL: "+this.getUrl().toString());
//    sb.append("\n");
//    sb.append("Title: "+this.getTitle());
//    sb.append("No of Outlinks : " + this.getOutlinks().length);
//    sb.append("Status: " + this.getStatus());
//    
//    return sb.toString();
//  }
  
  
  
}
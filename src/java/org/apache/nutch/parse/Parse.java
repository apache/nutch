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

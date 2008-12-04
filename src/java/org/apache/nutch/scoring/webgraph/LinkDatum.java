package org.apache.nutch.scoring.webgraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A class for holding link information including the url, anchor text, a score,
 * the timestamp of the link and a link type.
 */
public class LinkDatum
  implements Writable {

  public final static byte INLINK = 1;
  public final static byte OUTLINK = 2;

  private String url = null;
  private String anchor = "";
  private float score = 0.0f;
  private long timestamp = 0L;
  private byte linkType = 0;

  /**
   * Default constructor, no url, timestamp, score, or link type.
   */
  public LinkDatum() {

  }

  /**
   * Creates a LinkDatum with a given url. Timestamp is set to current time.
   * 
   * @param url The link url.
   */
  public LinkDatum(String url) {
    this(url, "", System.currentTimeMillis());
  }

  /**
   * Creates a LinkDatum with a url and an anchor text. Timestamp is set to
   * current time.
   * 
   * @param url The link url.
   * @param anchor The link anchor text.
   */
  public LinkDatum(String url, String anchor) {
    this(url, anchor, System.currentTimeMillis());
  }

  public LinkDatum(String url, String anchor, long timestamp) {
    this.url = url;
    this.anchor = anchor;
    this.timestamp = timestamp;
  }

  public String getUrl() {
    return url;
  }

  public String getAnchor() {
    return anchor;
  }

  public void setAnchor(String anchor) {
    this.anchor = anchor;
  }

  public float getScore() {
    return score;
  }

  public void setScore(float score) {
    this.score = score;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public byte getLinkType() {
    return linkType;
  }

  public void setLinkType(byte linkType) {
    this.linkType = linkType;
  }

  public void readFields(DataInput in)
    throws IOException {
    url = Text.readString(in);
    anchor = Text.readString(in);
    score = in.readFloat();
    timestamp = in.readLong();
    linkType = in.readByte();
  }

  public void write(DataOutput out)
    throws IOException {
    Text.writeString(out, url);
    Text.writeString(out, anchor != null ? anchor : "");
    out.writeFloat(score);
    out.writeLong(timestamp);
    out.writeByte(linkType);
  }

  public String toString() {

    String type = (linkType == INLINK ? "inlink" : (linkType == OUTLINK)
      ? "outlink" : "unknown");
    return "url: " + url + ", anchor: " + anchor + ", score: " + score
      + ", timestamp: " + timestamp + ", link type: " + type;
  }
}

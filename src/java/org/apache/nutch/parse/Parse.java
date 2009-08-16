package org.apache.nutch.parse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseStatus;

public class Parse implements Writable {

  private String text;
  private String title;
  private Outlink[] outlinks;
  private ParseStatus parseStatus;

  public Parse() {
  }
  
  public Parse(String text, String title, Outlink[] outlinks,
                    ParseStatus parseStatus) {
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

  public ParseStatus getParseStatus() {
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

  public void setParseStatus(ParseStatus parseStatus) {
    this.parseStatus = parseStatus;
  }
  
  public void readFields(DataInput in) throws IOException {
    text = Text.readString(in);
    title = Text.readString(in);
    int numOutlinks = in.readInt();
    outlinks = new Outlink[numOutlinks];
    for (int i = 0; i < numOutlinks; i++) {
      outlinks[i] = Outlink.read(in);
    }
    parseStatus = ParseStatus.read(in);
  }

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, text);
    Text.writeString(out, title);
    out.writeInt(outlinks.length);
    for (Outlink outlink : outlinks) {
      outlink.write(out);
    }
    parseStatus.write(out);
  }
}

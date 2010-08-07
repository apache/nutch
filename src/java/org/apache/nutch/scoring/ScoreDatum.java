package org.apache.nutch.scoring;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.nutch.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class ScoreDatum implements Writable {

  private float score;
  private String url;
  private String anchor;
  private Map<String, byte[]> metaData = new HashMap<String, byte[]>();
  
  public ScoreDatum() { }
  
  public ScoreDatum(float score, String url, String anchor) {
    this.score = score;
    this.url = url;
    this.anchor = anchor;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    score = in.readFloat();
    url = Text.readString(in);
    anchor = Text.readString(in);
    metaData.clear();
    
    int size = WritableUtils.readVInt(in);
    for (int i = 0; i < size; i++) {
      String key = Text.readString(in);
      byte[] value = Bytes.readByteArray(in);
      metaData.put(key, value);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(score);
    Text.writeString(out, url);
    Text.writeString(out, anchor);
    
    WritableUtils.writeVInt(out, metaData.size());
    for (Entry<String, byte[]> e : metaData.entrySet()) {
      Text.writeString(out, e.getKey());
      Bytes.writeByteArray(out, e.getValue());
    }
  }
  
  public byte[] getMeta(String key) {
    return metaData.get(key);
  }
  
  public void setMeta(String key, byte[] value) {
    metaData.put(key, value);
  }
  
  public byte[] deleteMeta(String key) {
    return metaData.remove(key);
  }
  
  public float getScore() {
    return score;
  }
  
  public void setScore(float score) {
    this.score = score;
  }

  public String getUrl() {
    return url;
  }
  
  public void setUrl(String url) {
    this.url = url;
  }

  public String getAnchor() {
    return anchor;
  }
}

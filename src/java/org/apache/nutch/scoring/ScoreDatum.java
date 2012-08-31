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
  private int distance;
  private Map<String, byte[]> metaData = new HashMap<String, byte[]>();
  
  public ScoreDatum() { }
  
  public ScoreDatum(float score, String url, String anchor, int depth) {
    this.score = score;
    this.url = url;
    this.anchor = anchor;
    this.distance = depth;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    score = in.readFloat();
    url = Text.readString(in);
    anchor = Text.readString(in);
    distance = WritableUtils.readVInt(in);
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
    WritableUtils.writeVInt(out, distance);
    
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
  
  public int getDistance() {
    return distance;
  }

  @Override
  public String toString() {
    return "ScoreDatum [score=" + score + ", url=" + url + ", anchor=" + anchor
        + ", distance="+distance + ", metaData=" + metaData + "]";
  }
  
  
}

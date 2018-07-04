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
package org.apache.nutch.hostdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.Map.Entry;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 */
public class HostDatum implements Writable, Cloneable {
  protected int failures = 0;
  protected float score = 0;
  protected Date lastCheck = new Date(0);
  protected String homepageUrl = new String();

  protected MapWritable metaData = new MapWritable();

  // Records the number of times DNS look-up failed, may indicate host no longer exists
  protected int dnsFailures = 0;

  // Records the number of connection failures, may indicate our netwerk being blocked by firewall
  protected int connectionFailures = 0;

  protected int unfetched = 0;
  protected int fetched = 0;
  protected int notModified = 0;
  protected int redirTemp = 0;
  protected int redirPerm = 0;
  protected int gone = 0;

  public HostDatum() {
  }

  public HostDatum(float score) {
    this(score, new Date());
  }

  public HostDatum(float score, Date lastCheck) {
    this(score, lastCheck, new String());
  }

  public HostDatum(float score, Date lastCheck, String homepageUrl) {
    this.score =  score;
    this.lastCheck = lastCheck;
    this.homepageUrl = homepageUrl;
  }

  public void resetFailures() {
    setDnsFailures(0);
    setConnectionFailures(0);
  }

  public void setDnsFailures(Integer dnsFailures) {
    this.dnsFailures = dnsFailures;
  }

  public void setConnectionFailures(Integer connectionFailures) {
    this.connectionFailures = connectionFailures;
  }

  public void incDnsFailures() {
    this.dnsFailures++;
  }

  public void incConnectionFailures() {
    this.connectionFailures++;
  }

  public Integer numFailures() {
    return getDnsFailures() + getConnectionFailures();
  }

  public Integer getDnsFailures() {
    return dnsFailures;
  }

  public Integer getConnectionFailures() {
    return connectionFailures;
  }

  public void setScore(float score) {
    this.score = score;
  }

  public void setLastCheck() {
    setLastCheck(new Date());
  }

  public void setLastCheck(Date date) {
    lastCheck = date;
  }

  public boolean isEmpty() {
    return (lastCheck.getTime() == 0) ? true : false;
  }

  public float getScore() {
    return score;
  }

  public Integer numRecords() {
    return unfetched + fetched + gone + redirPerm + redirTemp + notModified;
  }

  public Date getLastCheck() {
    return lastCheck;
  }

  public boolean hasHomepageUrl() {
    return homepageUrl.length() > 0;
  }

  public String getHomepageUrl() {
    return homepageUrl;
  }

  public void setHomepageUrl(String homepageUrl) {
    this.homepageUrl = homepageUrl;
  }

  public void setUnfetched(int val) {
    unfetched = val;
  }

  public int getUnfetched() {
    return unfetched;
  }

  public void setFetched(int val) {
    fetched = val;
  }

  public int getFetched() {
    return fetched;
  }

  public void setNotModified(int val) {
    notModified = val;
  }

  public int getNotModified() {
    return notModified;
  }

  public void setRedirTemp(int val) {
    redirTemp = val;
  }

  public int getRedirTemp() {
    return redirTemp;
  }

  public void setRedirPerm(int val) {
    redirPerm = val;
  }

  public int getRedirPerm() {
    return redirPerm;
  }

  public void setGone(int val) {
    gone = val;
  }

  public int getGone() {
    return gone;
  }

  public void resetStatistics() {
    setUnfetched(0);
    setFetched(0);
    setGone(0);
    setRedirTemp(0);
    setRedirPerm(0);
    setNotModified(0);
  }

   public void setMetaData(org.apache.hadoop.io.MapWritable mapWritable) {
     this.metaData = new org.apache.hadoop.io.MapWritable(mapWritable);
   }

   /**
    * Add all metadata from other CrawlDatum to this CrawlDatum.
    *
    * @param other HostDatum
    */
   public void putAllMetaData(HostDatum other) {
     for (Entry<Writable, Writable> e : other.getMetaData().entrySet()) {
       getMetaData().put(e.getKey(), e.getValue());
     }
   }

  /**
   * returns a MapWritable if it was set or read in @see readFields(DataInput),
   * returns empty map in case CrawlDatum was freshly created (lazily instantiated).
   */
  public org.apache.hadoop.io.MapWritable getMetaData() {
    if (this.metaData == null) this.metaData = new org.apache.hadoop.io.MapWritable();
    return this.metaData;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    HostDatum result = (HostDatum)super.clone();
    result.score = score;
    result.lastCheck = lastCheck;
    result.homepageUrl = homepageUrl;

    result.dnsFailures = dnsFailures;
    result.connectionFailures = connectionFailures;

    result.unfetched = unfetched;
    result.fetched = fetched;
    result.notModified = notModified;
    result.redirTemp = redirTemp;
    result.redirPerm = redirPerm;
    result.gone = gone;

    result.metaData = metaData;

    return result;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    score = in.readFloat();
    lastCheck = new Date(in.readLong());
    homepageUrl = Text.readString(in);

    dnsFailures = in.readInt();
    connectionFailures = in.readInt();

    unfetched= in.readInt();
    fetched= in.readInt();
    notModified= in.readInt();
    redirTemp= in.readInt();
    redirPerm = in.readInt();
    gone = in.readInt();

    metaData = new org.apache.hadoop.io.MapWritable();
    metaData.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(score);
    out.writeLong(lastCheck.getTime());
    Text.writeString(out, homepageUrl);

    out.writeInt(dnsFailures);
    out.writeInt(connectionFailures);

    out.writeInt(unfetched);
    out.writeInt(fetched);
    out.writeInt(notModified);
    out.writeInt(redirTemp);
    out.writeInt(redirPerm);
    out.writeInt(gone);

    metaData.write(out);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(Integer.toString(getUnfetched()));
    buf.append("\t");
    buf.append(Integer.toString(getFetched()));
    buf.append("\t");
    buf.append(Integer.toString(getGone()));
    buf.append("\t");
    buf.append(Integer.toString(getRedirTemp()));
    buf.append("\t");
    buf.append(Integer.toString(getRedirPerm()));
    buf.append("\t");
    buf.append(Integer.toString(getNotModified()));
    buf.append("\t");
    buf.append(Integer.toString(numRecords()));
    buf.append("\t");
    buf.append(Integer.toString(getDnsFailures()));
    buf.append("\t");
    buf.append(Integer.toString(getConnectionFailures()));
    buf.append("\t");
    buf.append(Integer.toString(numFailures()));
    buf.append("\t");
    buf.append(Float.toString(score));
    buf.append("\t");
    buf.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(lastCheck));
    buf.append("\t");
    buf.append(homepageUrl);
    buf.append("\t");
    for (Entry<Writable, Writable> e : getMetaData().entrySet()) {
      buf.append(e.getKey().toString());
      buf.append(':');
      buf.append(e.getValue().toString());
      buf.append("|||");
    }
    return buf.toString();
  }
}

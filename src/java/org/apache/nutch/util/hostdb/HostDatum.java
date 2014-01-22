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

package org.apache.nutch.util.hostdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.CrawlDatum;

/**
 * Contains information of a Host
 */
public class HostDatum implements Writable, Cloneable {
  private static final String EMPTY_STRING = "";
  private static final byte CUR_VERSION = 1;
  private static final Date DEFAULT_DATE = new Date(0);

  private float score = 0;
  private Date lastCheck = DEFAULT_DATE;
  private String homepageUrl = EMPTY_STRING;

  // Records the number of times DNS look-up failed, may indicate host no longer exists
  private int dnsFailures = 0;

  // Records the number of connection failures, may indicate our network being blocked by firewall
  private int connectionFailures = 0;

  // Counts for various url statuses
  private HashMap<Byte, Integer> statCounts = new HashMap<Byte, Integer>();

  private MapWritable metaData = new MapWritable();

  public HostDatum() {
    resetStatistics();
  }

  public boolean isEmpty() {
    return lastCheck.getTime() == 0;
  }

  public float getScore() { return score; }
  public void setScore(float score) { this.score = score; }

  public Date getLastCheck() { return lastCheck; }
  public void setLastCheck() { setLastCheck(new Date()); }
  public void setLastCheck(Date date) { lastCheck = date; }

  public boolean hasHomepageUrl() { return homepageUrl.compareTo(EMPTY_STRING) != 0; }
  public String getHomepageUrl() { return homepageUrl; }
  public void setHomepageUrl(String homepageUrl) { this.homepageUrl = homepageUrl; }

  public int getDnsFailures() { return dnsFailures; }
  public void incDnsFailures() { this.dnsFailures++; }
  public void setDnsFailures(int i) { this.dnsFailures = i; }

  public int getConnectionFailures() { return connectionFailures; }
  public void setConnectionFailures(int i) { this.connectionFailures = i; }
  public int numFailures() { return getDnsFailures() + getConnectionFailures(); }

  public Integer getStat(byte key) { return statCounts.get(key); }
  public void setStat(byte key, int val) { statCounts.put(key, val); }

  public void addStat(byte key, HostDatum other) {
    setStat(key, getStat(key) + other.getStat(key));
  }

  public Integer numRecords() {
    return statCounts.get(CrawlDatum.STATUS_DB_UNFETCHED) +
        statCounts.get(CrawlDatum.STATUS_DB_FETCHED) +
        statCounts.get(CrawlDatum.STATUS_DB_NOTMODIFIED) +
        statCounts.get(CrawlDatum.STATUS_DB_REDIR_PERM) +
        statCounts.get(CrawlDatum.STATUS_DB_REDIR_TEMP) +
        statCounts.get(CrawlDatum.STATUS_DB_GONE);
  }

  public void resetStatistics() {
    statCounts.put(CrawlDatum.STATUS_DB_UNFETCHED,    0);
    statCounts.put(CrawlDatum.STATUS_DB_FETCHED,      0);
    statCounts.put(CrawlDatum.STATUS_DB_NOTMODIFIED,  0);
    statCounts.put(CrawlDatum.STATUS_DB_REDIR_PERM,   0);
    statCounts.put(CrawlDatum.STATUS_DB_REDIR_TEMP,   0);
    statCounts.put(CrawlDatum.STATUS_DB_GONE,         0);
  }

  /**
   * Returns a MapWritable if it was set or read in @see readFields(DataInput),
   * Returns empty map in case CrawlDatum was freshly created (lazily instantiated).
   */
  public MapWritable getMetaData() {
    if (this.metaData == null) this.metaData = new MapWritable();
    return this.metaData;
  }

  /**
   * Add all metadata from other HostDatum to this HostDatum.
   */
  public void putAllMetaData(HostDatum other) {
    for (Entry<Writable, Writable> e : other.getMetaData().entrySet()) {
      getMetaData().put(e.getKey(), e.getValue());
    }
  }

  public void setMetaData(MapWritable mapWritable) {
    this.metaData = new MapWritable(mapWritable);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte version = in.readByte();
    if (version > CUR_VERSION)          // check version
      throw new VersionMismatchException(CUR_VERSION, version);

    score = in.readFloat();
    lastCheck = new Date(in.readLong());
    homepageUrl = Text.readString(in);

    dnsFailures = in.readInt();
    connectionFailures = in.readInt();

    statCounts.put(CrawlDatum.STATUS_DB_UNFETCHED, in.readInt());
    statCounts.put(CrawlDatum.STATUS_DB_FETCHED, in.readInt());
    statCounts.put(CrawlDatum.STATUS_DB_NOTMODIFIED, in.readInt());
    statCounts.put(CrawlDatum.STATUS_DB_REDIR_PERM, in.readInt());
    statCounts.put(CrawlDatum.STATUS_DB_REDIR_TEMP, in.readInt());
    statCounts.put(CrawlDatum.STATUS_DB_GONE, in.readInt());

    metaData = new MapWritable();
    metaData.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(CUR_VERSION);           // store current version
    out.writeFloat(score);
    out.writeLong(lastCheck.getTime());
    Text.writeString(out, homepageUrl);

    out.writeInt(dnsFailures);
    out.writeInt(connectionFailures);

    out.writeInt(statCounts.get(CrawlDatum.STATUS_DB_UNFETCHED));
    out.writeInt(statCounts.get(CrawlDatum.STATUS_DB_FETCHED));
    out.writeInt(statCounts.get(CrawlDatum.STATUS_DB_NOTMODIFIED));
    out.writeInt(statCounts.get(CrawlDatum.STATUS_DB_REDIR_PERM));
    out.writeInt(statCounts.get(CrawlDatum.STATUS_DB_REDIR_TEMP));
    out.writeInt(statCounts.get(CrawlDatum.STATUS_DB_GONE));

    metaData.write(out);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("Version: " + CUR_VERSION + "\n");
    buf.append("Homepage url: ").append(homepageUrl).append("\n");
    buf.append("Score: ").append(score).append("\n");

    if(lastCheck != DEFAULT_DATE)
      buf.append("Last check: ").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(lastCheck)).append("\n");
    else
      buf.append("Last check: \n");

    buf.append("Total records: ").append(numRecords()).append("\n");
    buf.append("  Unfetched: ").append(statCounts.get(CrawlDatum.STATUS_DB_UNFETCHED)).append("\n");
    buf.append("  Fetched: ").append(statCounts.get(CrawlDatum.STATUS_DB_FETCHED)).append("\n");
    buf.append("  Gone: ").append(statCounts.get(CrawlDatum.STATUS_DB_GONE)).append("\n");
    buf.append("  Perm redirect: ").append(statCounts.get(CrawlDatum.STATUS_DB_REDIR_PERM)).append("\n");
    buf.append("  Temp redirect: ").append(statCounts.get(CrawlDatum.STATUS_DB_REDIR_TEMP)).append("\n");
    buf.append("  Not modified: ").append(statCounts.get(CrawlDatum.STATUS_DB_NOTMODIFIED)).append("\n");

    buf.append("Total failures: ").append(numFailures()).append("\n");
    buf.append("  DNS failures: ").append(getDnsFailures()).append("\n");
    buf.append("  Connection failures: ").append(getConnectionFailures()).append("\n");

    return buf.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof HostDatum))
      return false;

    HostDatum other = (HostDatum) o;
    if(this.score == other.score &&
        this.lastCheck == other.lastCheck &&
        this.homepageUrl.compareTo(other.homepageUrl) == 0 &&
        this.dnsFailures == other.dnsFailures &&
        this.connectionFailures == other.connectionFailures) {
      for(Byte key : statCounts.keySet()) {
        if(other.getStat(key) == null || other.getStat(key).equals(statCounts.get(key)))
          return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return dnsFailures ^
        homepageUrl.hashCode() ^
        lastCheck.hashCode() ^
        connectionFailures ^
        Float.valueOf(score).hashCode() ^
        statCounts.get(CrawlDatum.STATUS_DB_UNFETCHED) ^
        statCounts.get(CrawlDatum.STATUS_DB_FETCHED) ^
        statCounts.get(CrawlDatum.STATUS_DB_NOTMODIFIED) ^
        statCounts.get(CrawlDatum.STATUS_DB_REDIR_PERM) ^
        statCounts.get(CrawlDatum.STATUS_DB_REDIR_TEMP) ^
        statCounts.get(CrawlDatum.STATUS_DB_GONE);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    HostDatum result = (HostDatum)super.clone();
    result.score = score;
    result.lastCheck = lastCheck;
    result.homepageUrl = homepageUrl;

    result.dnsFailures = dnsFailures;
    result.connectionFailures = connectionFailures;

    result.setStat(CrawlDatum.STATUS_DB_UNFETCHED, statCounts.get(CrawlDatum.STATUS_DB_UNFETCHED));
    result.setStat(CrawlDatum.STATUS_DB_FETCHED, statCounts.get(CrawlDatum.STATUS_DB_FETCHED));
    result.setStat(CrawlDatum.STATUS_DB_NOTMODIFIED, statCounts.get(CrawlDatum.STATUS_DB_NOTMODIFIED));
    result.setStat(CrawlDatum.STATUS_DB_REDIR_PERM, statCounts.get(CrawlDatum.STATUS_DB_REDIR_PERM));
    result.setStat(CrawlDatum.STATUS_DB_REDIR_TEMP, statCounts.get(CrawlDatum.STATUS_DB_REDIR_TEMP));
    result.setStat(CrawlDatum.STATUS_DB_GONE, statCounts.get(CrawlDatum.STATUS_DB_GONE));

    result.metaData = metaData;

    return result;
  }
}

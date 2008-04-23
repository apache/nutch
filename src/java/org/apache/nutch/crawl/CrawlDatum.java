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

package org.apache.nutch.crawl;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.nutch.util.*;

/* The crawl state of a url. */
public class CrawlDatum implements WritableComparable, Cloneable {
  public static final String GENERATE_DIR_NAME = "crawl_generate";
  public static final String FETCH_DIR_NAME = "crawl_fetch";
  public static final String PARSE_DIR_NAME = "crawl_parse";

  private final static byte CUR_VERSION = 6;

  /** Compatibility values for on-the-fly conversion from versions < 5. */
  private static final byte OLD_STATUS_SIGNATURE = 0;
  private static final byte OLD_STATUS_DB_UNFETCHED = 1;
  private static final byte OLD_STATUS_DB_FETCHED = 2;
  private static final byte OLD_STATUS_DB_GONE = 3;
  private static final byte OLD_STATUS_LINKED = 4;
  private static final byte OLD_STATUS_FETCH_SUCCESS = 5;
  private static final byte OLD_STATUS_FETCH_RETRY = 6;
  private static final byte OLD_STATUS_FETCH_GONE = 7;
  
  private static HashMap<Byte, Byte> oldToNew = new HashMap<Byte, Byte>();
  
  /** Page was not fetched yet. */
  public static final byte STATUS_DB_UNFETCHED      = 0x01;
  /** Page was successfully fetched. */
  public static final byte STATUS_DB_FETCHED        = 0x02;
  /** Page no longer exists. */
  public static final byte STATUS_DB_GONE           = 0x03;
  /** Page temporarily redirects to other page. */
  public static final byte STATUS_DB_REDIR_TEMP     = 0x04;
  /** Page permanently redirects to other page. */
  public static final byte STATUS_DB_REDIR_PERM     = 0x05;
  /** Page was successfully fetched and found not modified. */
  public static final byte STATUS_DB_NOTMODIFIED    = 0x06;
  
  /** Maximum value of DB-related status. */
  public static final byte STATUS_DB_MAX            = 0x1f;
  
  /** Fetching was successful. */
  public static final byte STATUS_FETCH_SUCCESS     = 0x21;
  /** Fetching unsuccessful, needs to be retried (transient errors). */
  public static final byte STATUS_FETCH_RETRY       = 0x22;
  /** Fetching temporarily redirected to other page. */
  public static final byte STATUS_FETCH_REDIR_TEMP  = 0x23;
  /** Fetching permanently redirected to other page. */
  public static final byte STATUS_FETCH_REDIR_PERM  = 0x24;
  /** Fetching unsuccessful - page is gone. */
  public static final byte STATUS_FETCH_GONE        = 0x25;
  /** Fetching successful - page is not modified. */
  public static final byte STATUS_FETCH_NOTMODIFIED = 0x26;
  
  /** Maximum value of fetch-related status. */
  public static final byte STATUS_FETCH_MAX         = 0x3f;
  
  /** Page signature. */
  public static final byte STATUS_SIGNATURE         = 0x41;
  /** Page was newly injected. */
  public static final byte STATUS_INJECTED          = 0x42;
  /** Page discovered through a link. */
  public static final byte STATUS_LINKED            = 0x43;
  
  
  public static final HashMap<Byte, String> statNames = new HashMap<Byte, String>();
  static {
    statNames.put(STATUS_DB_UNFETCHED, "db_unfetched");
    statNames.put(STATUS_DB_FETCHED, "db_fetched");
    statNames.put(STATUS_DB_GONE, "db_gone");
    statNames.put(STATUS_DB_REDIR_TEMP, "db_redir_temp");
    statNames.put(STATUS_DB_REDIR_PERM, "db_redir_perm");
    statNames.put(STATUS_DB_NOTMODIFIED, "db_notmodified");
    statNames.put(STATUS_SIGNATURE, "signature");
    statNames.put(STATUS_INJECTED, "injected");
    statNames.put(STATUS_LINKED, "linked");
    statNames.put(STATUS_FETCH_SUCCESS, "fetch_success");
    statNames.put(STATUS_FETCH_RETRY, "fetch_retry");
    statNames.put(STATUS_FETCH_REDIR_TEMP, "fetch_redir_temp");
    statNames.put(STATUS_FETCH_REDIR_PERM, "fetch_redir_perm");
    statNames.put(STATUS_FETCH_GONE, "fetch_gone");
    statNames.put(STATUS_FETCH_NOTMODIFIED, "fetch_notmodified");
    
    oldToNew.put(OLD_STATUS_DB_UNFETCHED, STATUS_DB_UNFETCHED);
    oldToNew.put(OLD_STATUS_DB_FETCHED, STATUS_DB_FETCHED);
    oldToNew.put(OLD_STATUS_DB_GONE, STATUS_DB_GONE);
    oldToNew.put(OLD_STATUS_FETCH_GONE, STATUS_FETCH_GONE);
    oldToNew.put(OLD_STATUS_FETCH_SUCCESS, STATUS_FETCH_SUCCESS);
    oldToNew.put(OLD_STATUS_FETCH_RETRY, STATUS_FETCH_RETRY);
    oldToNew.put(OLD_STATUS_LINKED, STATUS_LINKED);
    oldToNew.put(OLD_STATUS_SIGNATURE, STATUS_SIGNATURE);
  }

  private byte status;
  private long fetchTime = System.currentTimeMillis();
  private byte retries;
  private int fetchInterval;
  private float score = 1.0f;
  private byte[] signature = null;
  private long modifiedTime;
  private MapWritable metaData;
  
  public static boolean hasDbStatus(CrawlDatum datum) {
    if (datum.status <= STATUS_DB_MAX) return true;
    return false;
  }

  public static boolean hasFetchStatus(CrawlDatum datum) {
    if (datum.status > STATUS_DB_MAX && datum.status <= STATUS_FETCH_MAX) return true;
    return false;
  }

  public CrawlDatum() {
    metaData = new MapWritable();
  }

  public CrawlDatum(int status, int fetchInterval) {
    this.status = (byte)status;
    this.fetchInterval = fetchInterval;
  }

  public CrawlDatum(int status, int fetchInterval, float score) {
    this(status, fetchInterval);
    this.score = score;
  }

  //
  // accessor methods
  //

  public byte getStatus() { return status; }
  
  public static String getStatusName(byte value) {
    String res = statNames.get(value);
    if (res == null) res = "unknown";
    return res;
  }
  
  public void setStatus(int status) { this.status = (byte)status; }

  /**
   * Returns either the time of the last fetch, or the next fetch time,
   * depending on whether Fetcher or CrawlDbReducer set the time.
   */
  public long getFetchTime() { return fetchTime; }
  /**
   * Sets either the time of the last fetch or the next fetch time,
   * depending on whether Fetcher or CrawlDbReducer set the time.
   */
  public void setFetchTime(long fetchTime) { this.fetchTime = fetchTime; }

  public long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }
  
  public byte getRetriesSinceFetch() { return retries; }
  public void setRetriesSinceFetch(int retries) {this.retries = (byte)retries;}

  public int getFetchInterval() { return fetchInterval; }
  public void setFetchInterval(int fetchInterval) {
    this.fetchInterval = fetchInterval;
  }
  public void setFetchInterval(float fetchInterval) {
    this.fetchInterval = Math.round(fetchInterval);
  }

  public float getScore() { return score; }
  public void setScore(float score) { this.score = score; }

  public byte[] getSignature() {
    return signature;
  }

  public void setSignature(byte[] signature) {
    if (signature != null && signature.length > 256)
      throw new RuntimeException("Max signature length (256) exceeded: " + signature.length);
    this.signature = signature;
  }
  
   public void setMetaData(MapWritable mapWritable) {this.metaData = mapWritable; }

  /**
   * returns a MapWritable if it was set or read in @see readFields(DataInput), 
   * returns empty map in case CrawlDatum was freshly created (lazily instantiated).
   */
  public MapWritable getMetaData() {
    if (this.metaData == null) this.metaData = new MapWritable();
    return this.metaData;
  }
  

  //
  // writable methods
  //

  public static CrawlDatum read(DataInput in) throws IOException {
    CrawlDatum result = new CrawlDatum();
    result.readFields(in);
    return result;
  }


  public void readFields(DataInput in) throws IOException {
    byte version = in.readByte();                 // read version
    if (version > CUR_VERSION)                   // check version
      throw new VersionMismatchException(CUR_VERSION, version);

    status = in.readByte();
    fetchTime = in.readLong();
    retries = in.readByte();
    if (version > 5) {
      fetchInterval = in.readInt();
    } else fetchInterval = Math.round(in.readFloat());
    score = in.readFloat();
    if (version > 2) {
      modifiedTime = in.readLong();
      int cnt = in.readByte();
      if (cnt > 0) {
        signature = new byte[cnt];
        in.readFully(signature);
      } else signature = null;
    }
    if (version > 3) {
      metaData.clear();
      if (in.readBoolean()) {
        metaData.readFields(in);
      }
    }
    // translate status codes
    if (version < 5) {
      if (oldToNew.containsKey(status))
        status = oldToNew.get(status);
      else
        status = STATUS_DB_UNFETCHED;
      
    }
  }

  /** The number of bytes into a CrawlDatum that the score is stored. */
  private static final int SCORE_OFFSET = 1 + 1 + 8 + 1 + 4;
  private static final int SIG_OFFSET = SCORE_OFFSET + 4 + 8;

  public void write(DataOutput out) throws IOException {
    out.writeByte(CUR_VERSION);                   // store current version
    out.writeByte(status);
    out.writeLong(fetchTime);
    out.writeByte(retries);
    out.writeInt(fetchInterval);
    out.writeFloat(score);
    out.writeLong(modifiedTime);
    if (signature == null) {
      out.writeByte(0);
    } else {
      out.writeByte(signature.length);
      out.write(signature);
    }
    if (metaData != null && metaData.size() > 0) {
      out.writeBoolean(true);
      metaData.write(out);
    } else {
      out.writeBoolean(false);
    }
  }

  /** Copy the contents of another instance into this instance. */
  public void set(CrawlDatum that) {
    this.status = that.status;
    this.fetchTime = that.fetchTime;
    this.retries = that.retries;
    this.fetchInterval = that.fetchInterval;
    this.score = that.score;
    this.modifiedTime = that.modifiedTime;
    this.signature = that.signature;
    this.metaData = new MapWritable(that.metaData); // make a deep copy
  }


  //
  // compare methods
  //
  
  /** Sort by decreasing score. */
  public int compareTo(Object o) {
    CrawlDatum that = (CrawlDatum)o; 
    if (that.score != this.score)
      return (that.score - this.score) > 0 ? 1 : -1;
    if (that.status != this.status)
      return this.status - that.status;
    if (that.fetchTime != this.fetchTime)
      return (that.fetchTime - this.fetchTime) > 0 ? 1 : -1;
    if (that.retries != this.retries)
      return that.retries - this.retries;
    if (that.fetchInterval != this.fetchInterval)
      return (that.fetchInterval - this.fetchInterval) > 0 ? 1 : -1;
    if (that.modifiedTime != this.modifiedTime)
      return (that.modifiedTime - this.modifiedTime) > 0 ? 1 : -1;
    return SignatureComparator._compare(this, that);
  }

  /** A Comparator optimized for CrawlDatum. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() { super(CrawlDatum.class); }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      float score1 = readFloat(b1,s1+SCORE_OFFSET);
      float score2 = readFloat(b2,s2+SCORE_OFFSET);
      if (score2 != score1) {
        return (score2 - score1) > 0 ? 1 : -1;
      }
      int status1 = b1[s1+1];
      int status2 = b2[s2+1];
      if (status2 != status1)
        return status1 - status2;
      long fetchTime1 = readLong(b1, s1+1+1);
      long fetchTime2 = readLong(b2, s2+1+1);
      if (fetchTime2 != fetchTime1)
        return (fetchTime2 - fetchTime1) > 0 ? 1 : -1;
      int retries1 = b1[s1+1+1+8];
      int retries2 = b2[s2+1+1+8];
      if (retries2 != retries1)
        return retries2 - retries1;
      int fetchInterval1 = readInt(b1, s1+1+1+8+1);
      int fetchInterval2 = readInt(b2, s2+1+1+8+1);
      if (fetchInterval2 != fetchInterval1)
        return (fetchInterval2 - fetchInterval1) > 0 ? 1 : -1;
      long modifiedTime1 = readLong(b1, s1 + SCORE_OFFSET + 4);
      long modifiedTime2 = readLong(b2, s2 + SCORE_OFFSET + 4);
      if (modifiedTime2 != modifiedTime1)
        return (modifiedTime2 - modifiedTime1) > 0 ? 1 : -1;
      int sigl1 = b1[s1+SIG_OFFSET];
      int sigl2 = b2[s2+SIG_OFFSET];
      return SignatureComparator._compare(b1, SIG_OFFSET, sigl1, b2, SIG_OFFSET, sigl2);
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(CrawlDatum.class, new Comparator());
  }


  //
  // basic methods
  //

  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("Version: " + CUR_VERSION + "\n");
    buf.append("Status: " + getStatus() + " (" + getStatusName(getStatus()) + ")\n");
    buf.append("Fetch time: " + new Date(getFetchTime()) + "\n");
    buf.append("Modified time: " + new Date(getModifiedTime()) + "\n");
    buf.append("Retries since fetch: " + getRetriesSinceFetch() + "\n");
    buf.append("Retry interval: " + getFetchInterval() + " seconds (" +
        (getFetchInterval() / FetchSchedule.SECONDS_PER_DAY) + " days)\n");
    buf.append("Score: " + getScore() + "\n");
    buf.append("Signature: " + StringUtil.toHexString(getSignature()) + "\n");
    buf.append("Metadata: " + (metaData != null ? metaData.toString() : "null") + "\n");
    return buf.toString();
  }

  public boolean equals(Object o) {
    if (!(o instanceof CrawlDatum))
      return false;
    CrawlDatum other = (CrawlDatum)o;
    boolean res =
      (this.status == other.status) &&
      (this.fetchTime == other.fetchTime) &&
      (this.modifiedTime == other.modifiedTime) &&
      (this.retries == other.retries) &&
      (this.fetchInterval == other.fetchInterval) &&
      (SignatureComparator._compare(this.signature, other.signature) == 0) &&
      (this.score == other.score);
    if (!res) return res;
    // allow zero-sized metadata to be equal to null metadata
    if (this.metaData == null) {
      if (other.metaData != null && other.metaData.size() > 0) return false;
      else return true;
    } else {
      if (other.metaData == null) {
        if (this.metaData.size() == 0) return true;
        else return false;
      } else {
        return this.metaData.equals(other.metaData);
      }
    }
  }

  public int hashCode() {
    int res = 0;
    if (signature != null) {
      for (int i = 0; i < signature.length / 4; i += 4) {
        res ^= (int)(signature[i] << 24 + signature[i+1] << 16 +
                signature[i+2] << 8 + signature[i+3]);
      }
    }
    if (metaData != null) res ^= metaData.hashCode();
    return
      res ^ status ^
      ((int)fetchTime) ^
      ((int)modifiedTime) ^
      retries ^
      fetchInterval ^
      Float.floatToIntBits(score);
  }

  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
}

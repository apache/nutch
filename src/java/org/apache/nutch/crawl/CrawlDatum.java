/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.net.*;
import java.util.*;

import org.apache.nutch.io.*;
import org.apache.nutch.util.*;

/* The crawl state of a url. */
public class CrawlDatum implements WritableComparable, Cloneable {
  public static final String DB_DIR_NAME = "current";

  public static final String GENERATE_DIR_NAME = "crawl_generate";
  public static final String FETCH_DIR_NAME = "crawl_fetch";
  public static final String PARSE_DIR_NAME = "crawl_parse";

  private final static byte CUR_VERSION = 2;

  public static final byte STATUS_DB_UNFETCHED = 1;
  public static final byte STATUS_DB_FETCHED = 2;
  public static final byte STATUS_DB_GONE = 3;
  public static final byte STATUS_LINKED = 4;
  public static final byte STATUS_FETCH_SUCCESS = 5;
  public static final byte STATUS_FETCH_RETRY = 6;
  public static final byte STATUS_FETCH_GONE = 7;
  
  public static final String[] statNames = {
    "INVALID",
    "DB_unfetched",
    "DB_fetched",
    "DB_gone",
    "linked",
    "fetch_success",
    "fetch_retry",
    "fetch_gone"
  };

  private static final float MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000;

  private byte status;
  private long fetchTime = System.currentTimeMillis();
  private byte retries;
  private float fetchInterval;
  private float score = 1.0f;

  public CrawlDatum() {}

  public CrawlDatum(int status, float fetchInterval) {
    this.status = (byte)status;
    this.fetchInterval = fetchInterval;
  }

  public CrawlDatum(int status, float fetchInterval, float score) {
    this(status, fetchInterval);
    this.score = score;
  }

  //
  // accessor methods
  //

  public byte getStatus() { return status; }
  public void setStatus(int status) { this.status = (byte)status; }

  public long getFetchTime() { return fetchTime; }
  public void setFetchTime(long fetchTime) { this.fetchTime = fetchTime; }

  public void setNextFetchTime() {
    fetchTime += (long)(MILLISECONDS_PER_DAY*fetchInterval);
  }

  public byte getRetriesSinceFetch() { return retries; }
  public void setRetriesSinceFetch(int retries) {this.retries = (byte)retries;}

  public float getFetchInterval() { return fetchInterval; }
  public void setFetchInterval(float fetchInterval) {
    this.fetchInterval = fetchInterval;
  }

  public float getScore() { return score; }
  public void setScore(float score) { this.score = score; }

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
    if (version != CUR_VERSION)                   // check version
      throw new VersionMismatchException(CUR_VERSION, version);

    status = in.readByte();
    fetchTime = in.readLong();
    retries = in.readByte();
    fetchInterval = in.readFloat();
    score = in.readFloat();
  }

  /** The number of bytes into a CrawlDatum that the score is stored. */
  private static final int SCORE_OFFSET = 1 + 1 + 8 + 1 + 4;

  public void write(DataOutput out) throws IOException {
    out.writeByte(CUR_VERSION);                   // store current version
    out.writeByte(status);
    out.writeLong(fetchTime);
    out.writeByte(retries);
    out.writeFloat(fetchInterval);
    out.writeFloat(score);
  }

  /** Copy the contents of another instance into this instance. */
  public void set(CrawlDatum that) {
    this.status = that.status;
    this.fetchTime = that.fetchTime;
    this.retries = that.retries;
    this.fetchInterval = that.fetchInterval;
    this.score = that.score;
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
    return 0;
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
      float fetchInterval1 = readFloat(b1, s1+1+1+8+1);
      float fetchInterval2 = readFloat(b2, s2+1+1+8+1);
      if (fetchInterval2 != fetchInterval1)
        return (fetchInterval2 - fetchInterval1) > 0 ? 1 : -1;
      return 0;
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
    buf.append("Status: " + getStatus() + " (" + statNames[getStatus()] + ")\n");
    buf.append("Fetch time: " + new Date(getFetchTime()) + "\n");
    buf.append("Retries since fetch: " + getRetriesSinceFetch() + "\n");
    buf.append("Retry interval: " + getFetchInterval() + " days\n");
    buf.append("Score: " + getScore() + "\n");
    return buf.toString();
  }

  public boolean equals(Object o) {
    if (!(o instanceof CrawlDatum))
      return false;
    CrawlDatum other = (CrawlDatum)o;
    return
      (this.status == other.status) &&
      (this.fetchTime == other.fetchTime) &&
      (this.retries == other.retries) &&
      (this.fetchInterval == other.fetchInterval) &&
      (this.score == other.score);
  }

  public int hashCode() {
    return
      status ^
      ((int)fetchTime) ^
      retries ^
      Float.floatToIntBits(fetchInterval) ^
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

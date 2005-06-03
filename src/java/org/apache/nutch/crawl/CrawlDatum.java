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

  private final static byte CUR_VERSION = 1;

  public static final byte STATUS_DB_UNFETCHED = 1;
  public static final byte STATUS_DB_FETCHED = 2;
  public static final byte STATUS_DB_GONE = 3;
  public static final byte STATUS_LINKED = 4;
  public static final byte STATUS_FETCH_SUCCESS = 5;
  public static final byte STATUS_FETCH_RETRY = 6;
  public static final byte STATUS_FETCH_GONE = 7;

  private byte status;
  private long nextFetch = System.currentTimeMillis();
  private byte retries;
  private float fetchInterval;
  private int linkCount;

  public CrawlDatum() {}

  public CrawlDatum(int status, float fetchInterval) {
    this.status = (byte)status;
    this.fetchInterval = fetchInterval;
    if (status == STATUS_LINKED)
      linkCount = 1;
  }

  //
  // accessor methods
  //

  public byte getStatus() { return status; }
  public void setStatus(int status) { this.status = (byte)status; }

  public long getNextFetchTime() { return nextFetch; }
  public void setNextFetchTime(long nextFetch) { this.nextFetch = nextFetch; }

  public byte getRetriesSinceFetch() { return retries; }
  public void setRetriesSinceFetch(int retries) {this.retries = (byte)retries;}

  public float getFetchInterval() { return fetchInterval; }
  public void setFetchInterval(float fetchInterval) {
    this.fetchInterval = fetchInterval;
  }

  public int getLinkCount() { return linkCount; }
  public void setLinkCount(int linkCount) { this.linkCount = linkCount; }

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
    if (version > CUR_VERSION)                    // check version
      throw new VersionMismatchException(CUR_VERSION, version);

    status = in.readByte();
    nextFetch = in.readLong();
    retries = in.readByte();
    fetchInterval = in.readFloat();
    linkCount = in.readInt();
  }

  /** The number of bytes into a CrawlDatum that the linkCount is stored. */
  private static final int LINK_COUNT_OFFSET = 1 + 1 + 8 + 1 + 4;

  public void write(DataOutput out) throws IOException {
    out.writeByte(CUR_VERSION);                   // store current version
    out.writeByte(status);
    out.writeLong(nextFetch);
    out.writeByte(retries);
    out.writeFloat(fetchInterval);
    out.writeInt(linkCount);
  }

  /** Copy the contents of another instance into this instance. */
  public void set(CrawlDatum that) {
    this.status = that.status;
    this.nextFetch = that.nextFetch;
    this.retries = that.retries;
    this.fetchInterval = that.fetchInterval;
    this.linkCount = that.linkCount;
  }


  //
  // compare methods
  //
  
  /** Sort by decreasing link count. */
  public int compareTo(Object o) {
    int thisLinkCount = this.linkCount;
    int thatLinkCount = ((CrawlDatum)o).linkCount;
    return thatLinkCount - thisLinkCount;
  }

  /** A Comparator optimized for CrawlDatum. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() { super(CrawlDatum.class); }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int linkCount1 = readInt(b1,s1+LINK_COUNT_OFFSET);
      int linkCount2 = readInt(b2,s2+LINK_COUNT_OFFSET);
      return linkCount2 - linkCount1;
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
    buf.append("Status: " + getStatus() + "\n");
    buf.append("Next fetch: " + new Date(getNextFetchTime()) + "\n");
    buf.append("Retries since fetch: " + getRetriesSinceFetch() + "\n");
    buf.append("Retry interval: " + getFetchInterval() + " days\n");
    buf.append("Link Count: " + getLinkCount() + "\n");
    return buf.toString();
  }

  public boolean equals(Object o) {
    if (!(o instanceof CrawlDatum))
      return false;
    CrawlDatum other = (CrawlDatum)o;
    return
      (this.status == other.status) &&
      (this.nextFetch == other.nextFetch) &&
      (this.retries == other.retries) &&
      (this.fetchInterval == other.fetchInterval) &&
      (this.linkCount == other.linkCount);
  }

  public int hashCode() {
    return
      status ^
      ((int)nextFetch) ^
      retries ^
      Float.floatToIntBits(fetchInterval) ^
      linkCount;
  }

  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

}

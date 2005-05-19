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
public class CrawlDatum implements Writable, Cloneable {
  private final static byte CUR_VERSION = 0;

  public static final byte STATUS_DB_UNFETCHED = 0;
  public static final byte STATUS_DB_FETCHED = 1;
  public static final byte STATUS_LINKED = 2;
  public static final byte STATUS_FETCHER_SUCCESS = 3;
  public static final byte STATUS_FETCHER_FAIL_TEMP = 4;
  public static final byte STATUS_FETCHER_FAIL_PERM = 5;

  private byte status;
  private long nextFetch = System.currentTimeMillis();
  private byte retries;
  private float fetchInterval;

  public CrawlDatum() {}

  public CrawlDatum(int status, float fetchInterval) {
    this.status = (byte)status;
    this.fetchInterval = fetchInterval;
  }

  //
  // accessor methods
  //

  public byte getStatus() { return status; }
  public void setStatus(byte status) { this.status = (byte)status; }

  public long getNextFetchTime() { return nextFetch; }
  public void setNextFetchTime(long nextFetch) { this.nextFetch = nextFetch; }

  public byte getRetriesSinceFetch() { return retries; }
  public void setRetriesSinceFetch(int retries) {this.retries = (byte)retries;}

  public float getFetchInterval() { return fetchInterval; }
  public void setFetchInterval(float fetchInterval) {
    this.fetchInterval = fetchInterval;
  }

  //
  // writable methods
  //

  public void readFields(DataInput in) throws IOException {
    byte version = in.readByte();                 // read version
    if (version > CUR_VERSION)                    // check version
      throw new VersionMismatchException(CUR_VERSION, version);

    status = in.readByte();
    nextFetch = in.readLong();
    retries = in.readByte();
    fetchInterval = in.readFloat();
  }

  public void write(DataOutput out) throws IOException {
    out.writeByte(CUR_VERSION);                   // store current version
    out.write(status);
    out.writeLong(nextFetch);
    out.write(retries);
    out.writeFloat(fetchInterval);
  }

  /** Copy the contents of another instance into this instance. */
  public void set(CrawlDatum that) {
    this.status = that.status;
    this.nextFetch = that.nextFetch;
    this.retries = that.retries;
    this.fetchInterval = that.fetchInterval;
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
      (this.fetchInterval == other.fetchInterval);
  }

  public int hashCode() {
    return
      status ^
      ((int)nextFetch) ^
      retries ^
      Float.floatToIntBits(fetchInterval);
  }

  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

}

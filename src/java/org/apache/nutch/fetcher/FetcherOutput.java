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

package org.apache.nutch.fetcher;

import java.io.*;
import java.util.Arrays;
import java.util.Date;

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;
import org.apache.nutch.pagedb.FetchListEntry;
import org.apache.nutch.tools.UpdateDatabaseTool;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.protocol.ProtocolStatus;

/*********************************************
 * An entry in the fetcher's output.  This includes all of the fetcher output
 * except the raw and stripped versions of the content, which are placed in
 * separate files.
 *
 * <p>
 * Note by John Xing: As of 20041022, option -noParsing is introduced
 * in Fetcher.java. This changes fetcher behavior. Accordingly
 * there are necessary modifications in this class.
 * Check Fetcher.java and ParseSegment.java for details.
 *
 * @author Doug Cutting
 *********************************************/
public final class FetcherOutput implements Writable {
  public static final String DIR_NAME = "fetcher";
  // 20041024, xing, 
  // When fetcher is run with option -noParsing, DIR_NAME_NP is created
  // instead of DIR_NAME. In separate pass, ParseSegment.java looks for
  // DIR_NAME_NP and generates DIR_NAME. Check ParseSegment.java for more info.
  public static final String DIR_NAME_NP = DIR_NAME+"_output";
  public static final String DONE_NAME = "fetcher.done";
  public static final String ERROR_NAME = "fetcher.error";

  private final static byte VERSION = 5;

  // backwards compatibility codes
  private final static byte RETRY = 0;
  private final static byte SUCCESS = 1;
  private final static byte NOT_FOUND = 2;
  private final static byte CANT_PARSE = 4; // fetched, but can't be parsed
  
  private static final byte[] oldToNewMap = {
          ProtocolStatus.RETRY,
          ProtocolStatus.SUCCESS,
          ProtocolStatus.NOT_FOUND,
          ProtocolStatus.FAILED,
          ProtocolStatus.RETRY
  };

  private FetchListEntry fetchListEntry;
  private MD5Hash md5Hash;
  private ProtocolStatus protocolStatus;
  private long fetchDate;

  public FetcherOutput() {}

  public FetcherOutput(FetchListEntry fetchListEntry,
                       MD5Hash md5Hash, ProtocolStatus protocolStatus) {
    this.fetchListEntry = fetchListEntry;
    this.md5Hash = md5Hash;
    this.protocolStatus = protocolStatus;
    this.fetchDate = System.currentTimeMillis();
  }

  public byte getVersion() { return VERSION; }

  public final void readFields(DataInput in) throws IOException {
    byte version = in.readByte();                 // read version
    fetchListEntry = FetchListEntry.read(in);
    md5Hash = MD5Hash.read(in);
    if (version < 5) {
      int status = in.readByte();
      protocolStatus = new ProtocolStatus(oldToNewMap[status]);
    } else {
      protocolStatus = ProtocolStatus.read(in);
    }

    if (version < 4) {
      UTF8.readString(in);                        // read & ignore title
      int totalOutlinks = in.readInt();           // read & ignore outlinks
      for (int i = 0; i < totalOutlinks; i++) {
        Outlink.skip(in);
      }
    }

    fetchDate = (version > 1) ? in.readLong() : 0; // added in version=2
  }

  public final void write(DataOutput out) throws IOException {
    out.writeByte(VERSION);                       // store current version
    fetchListEntry.write(out);
    md5Hash.write(out);
    protocolStatus.write(out);
    out.writeLong(fetchDate);
  }

  public static FetcherOutput read(DataInput in) throws IOException {
    FetcherOutput fetcherOutput = new FetcherOutput();
    fetcherOutput.readFields(in);
    return fetcherOutput;
  }

  //
  // Accessor methods
  //
  public FetchListEntry getFetchListEntry() { return fetchListEntry; }
  public MD5Hash getMD5Hash() { return md5Hash; }
  public ProtocolStatus getProtocolStatus() { return protocolStatus; }
  public void setProtocolStatus(ProtocolStatus protocolStatus) { this.protocolStatus = protocolStatus; }
  public long getFetchDate() { return fetchDate; }
  public void setFetchDate(long fetchDate) { this.fetchDate = fetchDate; }

  // convenience methods
  public UTF8 getUrl() { return getFetchListEntry().getUrl(); }
  public String[] getAnchors() { return getFetchListEntry().getAnchors(); }

  public boolean equals(Object o) {
    if (!(o instanceof FetcherOutput))
      return false;
    FetcherOutput other = (FetcherOutput)o;
    return
      this.fetchListEntry.equals(other.fetchListEntry) &&
      this.md5Hash.equals(other.md5Hash) &&
      this.protocolStatus.equals(other.protocolStatus);
  }


  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("FetchListEntry: " + fetchListEntry + "Fetch Result:\n" );
    buffer.append("MD5Hash: " + md5Hash + "\n" );
    buffer.append("ProtocolStatus: " + protocolStatus + "\n" );
    buffer.append("FetchDate: " + new Date(fetchDate) + "\n" );
    return buffer.toString();
  }

  public static void main(String argv[]) throws Exception {
    String usage = "FetcherOutput (-local <path> | -ndfs <path> <namenode:port>) (-recno <recno> | -dumpall) [-filename <filename>]";
    if (argv.length == 0 || argv.length > 4) {
      System.out.println("usage:" + usage);
      return;
    }

    // Process the args
    String filename = FetcherOutput.DIR_NAME;
    boolean dumpall = false;
    int recno = -1;
    int i = 0;
    NutchFileSystem nfs = NutchFileSystem.parseArgs(argv, i);
    for (; i < argv.length; i++) {
        if ("-recno".equals(argv[i])) {
            recno = Integer.parseInt(argv[i+1]);
            i++;
        } else if ("-dumpall".equals(argv[i])) {
            dumpall = true;
        } else if ("-filename".equals(argv[i])) {
            filename = argv[i+1];
            i++;
        }
    }

    // Now carry out the command
    ArrayFile.Reader fetcher = new ArrayFile.Reader(nfs, filename);
    try {
      FetcherOutput fo = new FetcherOutput();

      if (dumpall) {
        while ((fo = (FetcherOutput) fetcher.next(fo)) != null) {
          recno++;
          System.out.println("Retrieved " + recno + " from file " + filename);
          System.out.println(fo);
        }
      } else if (recno >= 0) {
        fetcher.get(recno, fo);
        System.out.println("Retrieved " + recno + " from file " + filename);
        System.out.println(fo);
      }
    } finally {
      fetcher.close();
    }
  }
}

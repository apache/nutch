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

package org.apache.nutch.pagedb;

import java.io.*;
import java.util.Arrays;

import org.apache.nutch.io.*;
import org.apache.nutch.db.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;

public final class FetchListEntry implements Writable, Cloneable  {
  public static final String DIR_NAME = "fetchlist";

  private final static byte CUR_VERSION = 2;
    
  private boolean fetch;
  private Page page;
  private String[] anchors;

  public FetchListEntry() {}

  public FetchListEntry(boolean fetch, Page page, String[] anchors) {
    this.fetch = fetch;
    this.page = page;
    this.anchors = anchors;
  }

  //
  // Writable
  //
  public final void readFields(DataInput in) throws IOException {
    byte version = in.readByte();                 // read version
    if (version > CUR_VERSION)                    // check version
      throw new VersionMismatchException(CUR_VERSION, version);

    fetch = in.readByte() != 0;                   // read fetch flag

    page = Page.read(in);                         // read page

    if (version > 1) {                            // anchors added in version 2
      anchors = new String[in.readInt()];         // read anchors
      for (int i = 0; i < anchors.length; i++) {
        anchors[i] = UTF8.readString(in);
      }
    } else {
      anchors = new String[0];
    }
  }

  public static FetchListEntry read(DataInput in) throws IOException {
    FetchListEntry result = new FetchListEntry();
    result.readFields(in);
    return result;
  }

  public final void write(DataOutput out) throws IOException {
    out.writeByte(CUR_VERSION);                   // store current version
    out.writeByte((byte)(fetch ? 1 : 0));         // write fetch flag
    page.write(out);                              // write page
    out.writeInt(anchors.length);                 // write anchors
    for (int i = 0; i < anchors.length; i++) {
      UTF8.writeString(out, anchors[i]);
    }
  }

  //
  // Accessor methods
  //
  public boolean getFetch() { return fetch; }
  public Page getPage() { return page; }
  public String[] getAnchors() { return anchors; }
  public void setAnchors(String[] anchors) { this.anchors = anchors; }

  // convenience method
  public UTF8 getUrl() { return getPage().getURL(); }

  public boolean equals(Object o) {
    if (!(o instanceof FetchListEntry))
      return false;
    FetchListEntry other = (FetchListEntry)o;
    return
      this.fetch == other.fetch &&
      this.page.equals(other.page) &&
      Arrays.equals(this.anchors, other.anchors);
  }

  public Object clone() {
    try {
      FetchListEntry clone = (FetchListEntry)super.clone();
      clone.page = (Page)clone.page.clone();
      clone.anchors = new String[this.anchors.length];
      System.arraycopy(this.anchors, 0, clone.anchors, 0, this.anchors.length);
      return clone;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("version: " + CUR_VERSION + "\n");
    buffer.append("fetch: " + fetch + "\n");
    buffer.append("page: " + page + "\n");
    buffer.append("anchors: " + anchors.length + "\n" );
    for (int i = 0; i < anchors.length; i++) {
      buffer.append("  anchor: " + anchors[i] + "\n");
    }
    return buffer.toString();
  }

  public static void main(String argv[]) throws Exception {
    String usage = "FetchListEntry (-local | -ndfs <namenode:port>) [ -recno N | -dumpurls ] segmentDir";
    if (argv.length < 1) {
      System.out.println("Usage: " + usage);
      System.exit(-1);
    }
    String segment = null;
    boolean dumpUrls = false;
    int recno = -1;
    int i = 0;
    NutchFileSystem nfs = NutchFileSystem.parseArgs(argv, i);
    for (; i < argv.length; i++) {
      if ("-dumpurls".equals(argv[i])) {
        dumpUrls = true;
      } else if ("-recno".equals(argv[i])) {
        recno = Integer.parseInt(argv[++i]);
      } else if (argv[i] != null) {
        segment = argv[i];
      }
    }
    FetchListEntry fle = new FetchListEntry();
    ArrayFile.Reader fetchlist =
      new ArrayFile.Reader(nfs, new File(segment, FetchListEntry.DIR_NAME).getPath());

    if (dumpUrls) {
      int count = 0;
      while (fetchlist.next(fle) != null) {
        System.out.println("Recno " + count + ": " + fle.getPage().getURL());
        count++;
      }
    }

    if (recno != -1) {
      fetchlist.get(recno, fle);
      System.out.println(fle);
    }

    fetchlist.close();
  }

}

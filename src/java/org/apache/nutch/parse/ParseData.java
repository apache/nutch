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

package org.apache.nutch.parse;

import java.io.*;
import java.util.*;

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.protocol.ContentProperties;
import org.apache.nutch.tools.UpdateDatabaseTool;


/** Data extracted from a page's content.
 * @see Parse#getData()
 */
public final class ParseData extends VersionedWritable {
  public static final String DIR_NAME = "parse_data";

  private final static byte VERSION = 2;

  private String title;
  private Outlink[] outlinks;
  private ContentProperties metadata;
  private ParseStatus status;

  public ParseData() {}

  public ParseData(ParseStatus status, String title, Outlink[] outlinks, ContentProperties metadata) {
    this.status = status;
    this.title = title;
    this.outlinks = outlinks;
    this.metadata = metadata;
  }

  //
  // Accessor methods
  //

  /** The status of parsing the page. */
  public ParseStatus getStatus() { return status; }
  
  /** The title of the page. */
  public String getTitle() { return title; }

  /** The outlinks of the page. */
  public Outlink[] getOutlinks() { return outlinks; }

  /** Other page properties.  This is the place to find format-specific
   * properties.  Different parser implementations for different content types
   * will populate this differently. */
  public ContentProperties getMetadata() { return metadata; }

  /** Return the value of a metadata property. */
  public String get(String name) { return getMetadata().getProperty(name); }

  //
  // Writable methods
  //

  public byte getVersion() { return VERSION; }

  public final void readFields(DataInput in) throws IOException {

    byte version = in.readByte();
    if (version > 1)
      status = ParseStatus.read(in);
    else
      status = ParseStatus.STATUS_SUCCESS;
    title = UTF8.readString(in);                   // read title

    int totalOutlinks = in.readInt();             // read outlinks
    int outlinksToRead = Math.min(UpdateDatabaseTool.MAX_OUTLINKS_PER_PAGE,
                                  totalOutlinks);
    outlinks = new Outlink[outlinksToRead];
    for (int i = 0; i < outlinksToRead; i++) {
      outlinks[i] = Outlink.read(in);
    }
    for (int i = outlinksToRead; i < totalOutlinks; i++) {
      Outlink.skip(in);
    }
    
    int propertyCount = in.readInt();             // read metadata
    metadata = new ContentProperties();
    for (int i = 0; i < propertyCount; i++) {
      metadata.put(UTF8.readString(in), UTF8.readString(in));
    }
    
  }

  public final void write(DataOutput out) throws IOException {
    out.writeByte(VERSION);                             // write version
    status.write(out);                       // write status
    UTF8.writeString(out, title);                 // write title

    out.writeInt(outlinks.length);                // write outlinks
    for (int i = 0; i < outlinks.length; i++) {
      outlinks[i].write(out);
    }

    out.writeInt(metadata.size());                // write metadata
    Iterator i = metadata.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry e = (Map.Entry)i.next();
      UTF8.writeString(out, (String)e.getKey());
      UTF8.writeString(out, (String)e.getValue());
    }
  }

  public static ParseData read(DataInput in) throws IOException {
    ParseData parseText = new ParseData();
    parseText.readFields(in);
    return parseText;
  }

  //
  // other methods
  //

  public boolean equals(Object o) {
    if (!(o instanceof ParseData))
      return false;
    ParseData other = (ParseData)o;
    return
      this.status.equals(other.status) &&
      this.title.equals(other.title) &&
      Arrays.equals(this.outlinks, other.outlinks) &&
      this.metadata.equals(other.metadata);
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();

    buffer.append("Status: " + status + "\n" );
    buffer.append("Title: " + title + "\n" );

    if (outlinks != null) {
      buffer.append("Outlinks: " + outlinks.length + "\n" );
      for (int i = 0; i < outlinks.length; i++) {
        buffer.append("  outlink: " + outlinks[i] + "\n");
      }
    }

    buffer.append("Metadata: " + metadata + "\n" );

    return buffer.toString();
  }

  public static void main(String argv[]) throws Exception {
    String usage = "ParseData (-local | -ndfs <namenode:port>) recno segment";
    
    if (argv.length < 3) {
      System.out.println("usage:" + usage);
      return;
    }

    NutchFileSystem nfs = NutchFileSystem.parseArgs(argv, 0);
    try {
      int recno = Integer.parseInt(argv[0]);
      String segment = argv[1];

      File file = new File(segment, DIR_NAME);
      System.out.println("Reading from file: " + file);

      ArrayFile.Reader parses = new ArrayFile.Reader(nfs, file.toString());

      ParseData parseDatum = new ParseData();
      parses.get(recno, parseDatum);

      System.out.println("Retrieved " + recno + " from file " + file);
      System.out.println(parseDatum);

      parses.close();
    } finally {
      nfs.close();
    }
  }
}

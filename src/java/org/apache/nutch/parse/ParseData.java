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

package org.apache.nutch.parse;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.util.NutchConfiguration;


/** Data extracted from a page's content.
 * @see Parse#getData()
 */
public final class ParseData extends VersionedWritable {
  public static final String DIR_NAME = "parse_data";

  private final static byte VERSION = 5;

  private String title;
  private Outlink[] outlinks;
  private Metadata contentMeta;
  private Metadata parseMeta;
  private ParseStatus status;
  private byte version = VERSION;
  
  public ParseData() {
    contentMeta = new Metadata();
    parseMeta = new Metadata();
  }

  public ParseData(ParseStatus status, String title, Outlink[] outlinks,
                   Metadata contentMeta) {
    this(status, title, outlinks, contentMeta, new Metadata());
  }
  
  public ParseData(ParseStatus status, String title, Outlink[] outlinks,
                   Metadata contentMeta, Metadata parseMeta) {
    this.status = status;
    this.title = title;
    this.outlinks = outlinks;
    this.contentMeta = contentMeta;
    this.parseMeta = parseMeta;
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

  /** The original Metadata retrieved from content */
  public Metadata getContentMeta() { return contentMeta; }

  /**
   * Other content properties.
   * This is the place to find format-specific properties.
   * Different parser implementations for different content types will populate
   * this differently.
   */
  public Metadata getParseMeta() { return parseMeta; }
  
  public void setParseMeta(Metadata parseMeta) {
    this.parseMeta = parseMeta;
  }
  
  /**
   * Get a metadata single value.
   * This method first looks for the metadata value in the parse metadata. If no
   * value is found it the looks for the metadata in the content metadata.
   * @see #getContentMeta()
   * @see #getParseMeta()
   */
  public String getMeta(String name) {
    String value = parseMeta.get(name);
    if (value == null) {
      value = contentMeta.get(name);
    }
    return value;
  }
  
  //
  // Writable methods
  //

  public byte getVersion() { return version; }

  public final void readFields(DataInput in) throws IOException {

    version = in.readByte();
    // incompatible change from UTF8 (version < 5) to Text
    if (version != VERSION)
      throw new VersionMismatchException(VERSION, version);
    status = ParseStatus.read(in);
    title = Text.readString(in);                   // read title

    int numOutlinks = in.readInt();    
    outlinks = new Outlink[numOutlinks];
    for (int i = 0; i < numOutlinks; i++) {
      outlinks[i] = Outlink.read(in);
    }
    
    if (version < 3) {
      int propertyCount = in.readInt();             // read metadata
      contentMeta.clear();
      for (int i = 0; i < propertyCount; i++) {
        contentMeta.add(Text.readString(in), Text.readString(in));
      }
    } else {
      contentMeta.clear();
      contentMeta.readFields(in);
    }
    if (version > 3) {
      parseMeta.clear();
      parseMeta.readFields(in);
    }
  }

  public final void write(DataOutput out) throws IOException {
    out.writeByte(VERSION);                       // write version
    status.write(out);                            // write status
    Text.writeString(out, title);                 // write title

    out.writeInt(outlinks.length);                // write outlinks
    for (int i = 0; i < outlinks.length; i++) {
      outlinks[i].write(out);
    }
    contentMeta.write(out);                      // write content metadata
    parseMeta.write(out);
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
      this.contentMeta.equals(other.contentMeta) &&
      this.parseMeta.equals(other.parseMeta);
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();

    buffer.append("Version: " + version + "\n" );
    buffer.append("Status: " + status + "\n" );
    buffer.append("Title: " + title + "\n" );

    if (outlinks != null) {
      buffer.append("Outlinks: " + outlinks.length + "\n" );
      for (int i = 0; i < outlinks.length; i++) {
        buffer.append("  outlink: " + outlinks[i] + "\n");
      }
    }

    buffer.append("Content Metadata: " + contentMeta + "\n" );
    buffer.append("Parse Metadata: " + parseMeta + "\n" );

    return buffer.toString();
  }

  public static void main(String argv[]) throws Exception {
    String usage = "ParseData (-local | -dfs <namenode:port>) recno segment";
    
    if (argv.length < 3) {
      System.out.println("usage:" + usage);
      return;
    }

    Configuration conf = NutchConfiguration.create();
    FileSystem fs = FileSystem.parseArgs(argv, 0, conf);
    try {
      int recno = Integer.parseInt(argv[0]);
      String segment = argv[1];

      Path file = new Path(segment, DIR_NAME);
      System.out.println("Reading from file: " + file);

      ArrayFile.Reader parses = new ArrayFile.Reader(fs, file.toString(), conf);

      ParseData parseDatum = new ParseData();
      parses.get(recno, parseDatum);

      System.out.println("Retrieved " + recno + " from file " + file);
      System.out.println(parseDatum);

      parses.close();
    } finally {
      fs.close();
    }
  }

}

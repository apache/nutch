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
package org.apache.nutch.parse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.Options;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.VersionedWritable;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.util.NutchConfiguration;

/**
 * Data extracted from a page's content.
 * 
 * @see Parse#getData()
 */
public final class ParseData extends VersionedWritable {
  public static final String DIR_NAME = "parse_data";

  private static final byte VERSION = 5;

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

  /**
   * Get the status of parsing the page.
   * @return the {@link ParseStatus}
   */
  public ParseStatus getStatus() {
    return status;
  }

  /**
   * Get the title of the page.
   * @return the title as a {@link String}
   */
  public String getTitle() {
    return title;
  }

  /**
   * Get the outlinks of the page.
   * @return an array of {@link org.apache.nutch.parse.Outlink}'s
   */
  public Outlink[] getOutlinks() {
    return outlinks;
  }

  /**
   * The original {@link Metadata} retrieved from content
   * @return the original content {@link Metadata}
   */
  public Metadata getContentMeta() {
    return contentMeta;
  }

  /**
   * Other content properties. This is the place to find format-specific
   * properties. Different parser implementations for different content types
   * will populate this differently.
   * @return a {@link Metadata}
   */
  public Metadata getParseMeta() {
    return parseMeta;
  }

  public void setParseMeta(Metadata parseMeta) {
    this.parseMeta = parseMeta;
  }

  public void setOutlinks(Outlink[] outlinks) {
    this.outlinks = outlinks;
  }

  /**
   * Get a metadata single value. This method first looks for the metadata value
   * in the parse metadata. If no value is found it the looks for the metadata
   * in the content metadata.
   * @param name the metadata key for which to retrieve a value
   * @return the (string) metadata value
   * 
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

  public byte getVersion() {
    return version;
  }

  public final void readFields(DataInput in) throws IOException {

    version = in.readByte();
    // incompatible change from UTF8 (version < 5) to Text
    if (version != VERSION)
      throw new VersionMismatchException(VERSION, version);
    status = ParseStatus.read(in);
    title = Text.readString(in); // read title

    int numOutlinks = in.readInt();
    outlinks = new Outlink[numOutlinks];
    for (int i = 0; i < numOutlinks; i++) {
      outlinks[i] = Outlink.read(in);
    }

    contentMeta.clear();
    contentMeta.readFields(in);
    parseMeta.clear();
    parseMeta.readFields(in);
  }

  public final void write(DataOutput out) throws IOException {
    out.writeByte(VERSION); // write version
    status.write(out); // write status
    Text.writeString(out, title); // write title

    out.writeInt(outlinks.length); // write outlinks
    for (int i = 0; i < outlinks.length; i++) {
      outlinks[i].write(out);
    }
    contentMeta.write(out); // write content metadata
    parseMeta.write(out);
  }

  public static ParseData read(DataInput in) throws IOException {
    ParseData parseText = new ParseData();
    parseText.readFields(in);
    return parseText;
  }

  public boolean equals(Object o) {
    if (!(o instanceof ParseData))
      return false;
    ParseData other = (ParseData) o;
    return this.status.equals(other.status) && this.title.equals(other.title)
        && Arrays.equals(this.outlinks, other.outlinks)
        && this.contentMeta.equals(other.contentMeta)
        && this.parseMeta.equals(other.parseMeta);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();

    buffer.append("Version: ").append(version).append("\n");
    buffer.append("Status: ").append(status).append("\n");
    buffer.append("Title: ").append(title ).append("\n");

    if (outlinks != null) {
      buffer.append("Outlinks: ").append(outlinks.length).append("\n");
      for (int i = 0; i < outlinks.length; i++) {
        buffer.append("  outlink: ").append(outlinks[i]).append("\n");
      }
    }

    buffer.append("Content Metadata:\n  ")
        .append(contentMeta.toString("\n  ", " = ")).append("\n");
    buffer.append("Parse Metadata:\n  ")
        .append(parseMeta.toString("\n  ", " = ")).append("\n");

    return buffer.toString();
  }

  public static void main(String argv[]) throws Exception {
    String usage = "ParseData (-local | -dfs <namenode:port>) recno segment";

    if (argv.length < 3) {
      System.out.println("usage:" + usage);
      return;
    }

    Options opts = new Options();
    Configuration conf = NutchConfiguration.create();

    GenericOptionsParser parser = new GenericOptionsParser(conf, opts, argv);

    String[] remainingArgs = parser.getRemainingArgs();

    try (FileSystem fs = FileSystem.get(conf)) {
      int recno = Integer.parseInt(remainingArgs[0]);
      String segment = remainingArgs[1];

      Path file = new Path(segment, DIR_NAME);
      System.out.println("Reading from file: " + file);

      ArrayFile.Reader parses = new ArrayFile.Reader(fs, file.toString(), conf);

      ParseData parseDatum = new ParseData();
      parses.get(recno, parseDatum);

      System.out.println("Retrieved " + recno + " from file " + file);
      System.out.println(parseDatum);

      parses.close();
    }
  }

}

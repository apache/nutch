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

package org.apache.nutch.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayFile;
import org.apache.hadoop.io.CompressedWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.mime.MimeType;
import org.apache.nutch.util.mime.MimeTypeException;
import org.apache.nutch.util.mime.MimeTypes;

public final class Content extends CompressedWritable {

  public static final String DIR_NAME = "content";

  private final static byte VERSION = 2;

  private byte version;

  private String url;

  private String base;

  private byte[] content;

  private String contentType;

  private Metadata metadata;

  private boolean mimeTypeMagic;

  private MimeTypes mimeTypes;

  private boolean inflated;

  public Content() {
    inflated = false;
  }

  public Content(String url, String base, byte[] content, String contentType,
      Metadata metadata, Configuration conf) {

    if (url == null)
      throw new IllegalArgumentException("null url");
    if (base == null)
      throw new IllegalArgumentException("null base");
    if (content == null)
      throw new IllegalArgumentException("null content");
    if (metadata == null)
      throw new IllegalArgumentException("null metadata");

    this.url = url;
    this.base = base;
    this.content = content;
    this.metadata = metadata;
    this.mimeTypeMagic = conf.getBoolean("mime.type.magic", true);
    this.mimeTypes = MimeTypes.get(conf.get("mime.types.file"));
    this.contentType = getContentType(contentType, url, content);
    inflated = true;
  }

  public void ensureInflated() {
    if (inflated) {
      return;
    }
    super.ensureInflated();
    inflated = true;
  }

  protected final void readFieldsCompressed(DataInput in) throws IOException {
    version = in.readByte();
    metadata = new SpellCheckedMetadata();
    switch (version) {
    case 0:
    case 1:
      url = UTF8.readString(in); // read url
      base = UTF8.readString(in); // read base

      content = new byte[in.readInt()]; // read content
      in.readFully(content);

      contentType = UTF8.readString(in); // read contentType
      // reconstruct metadata
      int keySize = in.readInt();
      String key;
      for (int i = 0; i < keySize; i++) {
        key = UTF8.readString(in);
        int valueSize = in.readInt();
        for (int j = 0; j < valueSize; j++) {
          metadata.add(key, UTF8.readString(in));
        }
      }
      break;
    case VERSION:
      url = Text.readString(in); // read url
      base = Text.readString(in); // read base

      content = new byte[in.readInt()]; // read content
      in.readFully(content);

      contentType = Text.readString(in); // read contentType
      metadata.readFields(in); // read meta data
      break;
    default:
      throw new VersionMismatchException(VERSION, version);
    }

  }

  protected final void writeCompressed(DataOutput out) throws IOException {
    out.writeByte(VERSION);

    Text.writeString(out, url); // write url
    Text.writeString(out, base); // write base

    out.writeInt(content.length); // write content
    out.write(content);

    Text.writeString(out, contentType); // write contentType

    metadata.write(out); // write metadata
  }

  public static Content read(DataInput in) throws IOException {
    Content content = new Content();
    content.readFields(in);
    return content;
  }

  //
  // Accessor methods
  //

  /** The url fetched. */
  public String getUrl() {
    ensureInflated();
    return url;
  }

  /** The base url for relative links contained in the content.
   * Maybe be different from url if the request redirected.
   */
  public String getBaseUrl() {
    ensureInflated();
    return base;
  }

  /** The binary content retrieved. */
  public byte[] getContent() {
    ensureInflated();
    return content;
  }

  public void setContent(byte[] content) {
    ensureInflated();
    this.content = content;
  }

  /** The media type of the retrieved content.
   * @see <a href="http://www.iana.org/assignments/media-types/">
   *      http://www.iana.org/assignments/media-types/</a>
   */
  public String getContentType() {
    ensureInflated();
    return contentType;
  }

  public void setContentType(String contentType) {
    ensureInflated();
    this.contentType = contentType;
  }

  /** Other protocol-specific data. */
  public Metadata getMetadata() {
    ensureInflated();
    return metadata;
  }

  /** Other protocol-specific data. */
  public void setMetadata(Metadata metadata) {
    ensureInflated();
    this.metadata = metadata;
  }

  public boolean equals(Object o) {
    ensureInflated();
    if (!(o instanceof Content)) {
      return false;
    }
    Content that = (Content) o;
    that.ensureInflated();
    return this.url.equals(that.url) && this.base.equals(that.base)
        && Arrays.equals(this.getContent(), that.getContent())
        && this.contentType.equals(that.contentType)
        && this.metadata.equals(that.metadata);
  }

  public String toString() {
    ensureInflated();
    StringBuffer buffer = new StringBuffer();

    buffer.append("Version: " + version + "\n");
    buffer.append("url: " + url + "\n");
    buffer.append("base: " + base + "\n");
    buffer.append("contentType: " + contentType + "\n");
    buffer.append("metadata: " + metadata + "\n");
    buffer.append("Content:\n");
    buffer.append(new String(content)); // try default encoding

    return buffer.toString();

  }

  public static void main(String argv[]) throws Exception {

    String usage = "Content (-local | -dfs <namenode:port>) recno segment";

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

      ArrayFile.Reader contents = new ArrayFile.Reader(fs, file.toString(),
          conf);

      Content content = new Content();
      contents.get(recno, content);
      System.out.println("Retrieved " + recno + " from file " + file);

      System.out.println(content);

      contents.close();
    } finally {
      fs.close();
    }
  }

  private String getContentType(String typeName, String url, byte[] data) {
    MimeType type = null;
    try {
      typeName = MimeType.clean(typeName);
      type = typeName == null ? null : this.mimeTypes.forName(typeName);
    } catch (MimeTypeException mte) {
      // Seems to be a malformed mime type name...
    }

    if (typeName == null || type == null || !type.matches(url)) {
      // If no mime-type header, or cannot find a corresponding registered
      // mime-type, or the one found doesn't match the url pattern
      // it shouldbe, then guess a mime-type from the url pattern
      type = this.mimeTypes.getMimeType(url);
      typeName = type == null ? typeName : type.getName();
    }
    if (typeName == null || type == null
        || (this.mimeTypeMagic && type.hasMagic() && !type.matches(data))) {
      // If no mime-type already found, or the one found doesn't match
      // the magic bytes it should be, then, guess a mime-type from the
      // document content (magic bytes)
      type = this.mimeTypes.getMimeType(data);
      typeName = type == null ? typeName : type.getName();
    }
    return typeName;
  }

  /**
   * By calling this method caller forces the next access to any property (via
   * getters and setters) to check if decompressing of data is really required.
   */
  public void forceInflate() {
    inflated = false;
  }

}

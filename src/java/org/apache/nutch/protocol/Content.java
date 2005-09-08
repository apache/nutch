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

package org.apache.nutch.protocol;

import java.util.*;
import java.io.*;

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;

public final class Content extends CompressedWritable {

  public static final String DIR_NAME = "content";

  private final static byte VERSION = 1;

  private byte version;
  private String url;
  private String base;
  private byte[] content;
  private String contentType;
  private Properties metadata;

  public Content() {}
    
  public Content(String url, String base, byte[] content, String contentType,
                 Properties metadata){

    if (url == null) throw new IllegalArgumentException("null url");
    if (base == null) throw new IllegalArgumentException("null base");
    if (content == null) throw new IllegalArgumentException("null content");
    if (contentType == null) throw new IllegalArgumentException("null type");
    if (metadata == null) throw new IllegalArgumentException("null metadata");

    this.url = url;
    this.base = base;
    this.content = content;
    this.contentType = contentType;
    this.metadata = metadata;
  }

  protected final void readFieldsCompressed(DataInput in) throws IOException {
    version = in.readByte();
    if (version > VERSION)
      throw new VersionMismatchException(VERSION, version);

    url = UTF8.readString(in);                    // read url
    base = UTF8.readString(in);                   // read base

    content = new byte[in.readInt()];             // read content
    in.readFully(content);

    contentType = UTF8.readString(in);            // read contentType

    int propertyCount = in.readInt();             // read metadata
    metadata = new Properties();
    for (int i = 0; i < propertyCount; i++) {
      metadata.put(UTF8.readString(in), UTF8.readString(in));
    }
  }

  protected final void writeCompressed(DataOutput out) throws IOException {
    out.writeByte(version);

    UTF8.writeString(out, url);                   // write url
    UTF8.writeString(out, base);                  // write base

    out.writeInt(content.length);                 // write content
    out.write(content);

    UTF8.writeString(out, contentType);           // write contentType
    
    out.writeInt(metadata.size());                // write metadata
    Iterator i = metadata.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry e = (Map.Entry)i.next();
      UTF8.writeString(out, (String)e.getKey());
      UTF8.writeString(out, (String)e.getValue());
    }
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
  public Properties getMetadata() {
    ensureInflated();
    return metadata;
  }

  /** Return the value of a metadata property. */
  public String get(String name) {
    ensureInflated();
    return getMetadata().getProperty(name);
  }

  public boolean equals(Object o) {
    ensureInflated();
    if (!(o instanceof Content)){
      return false;
    }
    Content that = (Content)o;
    that.ensureInflated();
    return
      this.url.equals(that.url) &&
      this.base.equals(that.base) &&
      Arrays.equals(this.getContent(), that.getContent()) &&
      this.contentType.equals(that.contentType) &&
      this.metadata.equals(that.metadata);
  }

  public String toString() {
    ensureInflated();
    StringBuffer buffer = new StringBuffer();

    buffer.append("url: " + url + "\n" );
    buffer.append("base: " + base + "\n" );
    buffer.append("contentType: " + contentType + "\n" );
    buffer.append("metadata: " + metadata + "\n" );
    buffer.append("Content:\n");
    buffer.append(new String(content));           // try default encoding

    return buffer.toString();

  }

  public static void main(String argv[]) throws Exception {

    String usage = "Content (-local | -ndfs <namenode:port>) recno segment";
    
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

      ArrayFile.Reader contents = new ArrayFile.Reader(nfs, file.toString());

      Content content = new Content();
      contents.get(recno, content);
      System.out.println("Retrieved " + recno + " from file " + file);

      System.out.println(content);

      contents.close();
    } finally {
      nfs.close();
    }
  }
}

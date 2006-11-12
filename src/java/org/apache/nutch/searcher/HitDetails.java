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

package org.apache.nutch.searcher;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.*;
import org.apache.nutch.html.Entities;

/** Data stored in the index for a hit.
 *
 * <p>Represented as a list of name/value pairs.
 */
public final class HitDetails implements Writable {

  private int length;
  private String[] fields;
  private String[] values;

  public HitDetails() {}

  /** Construct from field names and values arrays. */
  public HitDetails(String[] fields, String[] values) {
    this.length = fields.length;
    this.fields = fields;
    this.values = values;
  }

  /** Construct minimal details from a segment name and document number. */
  public HitDetails(String segment, String url) {
    this(new String[2], new String[2]);
    this.fields[0] = "segment";
    this.values[0] = segment;
    this.fields[1] = "url";
    this.values[1] = url;
  }

  /** Returns the number of fields contained in this. */
  public int getLength() { return length; }

  /** Returns the name of the <code>i</code><sup>th</sup> field. */
  public String getField(int i) { return fields[i]; }

  /** Returns the value of the <code>i</code><sup>th</sup> field. */
  public String getValue(int i) { return values[i]; }
  
  /** Returns the value of the first field with the specified name. */
  public String getValue(String field) {
    for (int i = 0; i < length; i++) {
      if (fields[i].equals(field))
        return values[i];
    }
    return null;
  }

  /** Returns all the values with the specified name. */
  public String[] getValues(String field) {
   ArrayList vals = new ArrayList();
   for (int i=0; i<length; i++) {
     if (fields[i].equals(field)) {
       vals.add(values[i]);
     }
   }
   return (vals.size() > 0)
            ? (String[]) vals.toArray(new String[vals.size()])
            : null;
}

  // javadoc from Writable
  public void write(DataOutput out) throws IOException {
    out.writeInt(length);
    for (int i = 0; i < length; i++) {
      out.writeUTF(fields[i]);
      out.writeUTF(values[i]);
    }
  }
  
  /** Constructs, reads and returns an instance. */
  public static HitDetails read(DataInput in) throws IOException {
    HitDetails result = new HitDetails();
    result.readFields(in);
    return result;
  }

  // javadoc from Writable
  public void readFields(DataInput in) throws IOException {
    length = in.readInt();
    fields = new String[length];
    values = new String[length];
    for (int i = 0; i < length; i++) {
      fields[i] = in.readUTF();
      values[i] = in.readUTF();
    }
  }

  /** Display as a string. */
  public String toString() {
    return getValue("segment") + "/" + getValue("url");
  }

  /** Display as HTML. */
  public String toHtml() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("<ul>\n");
    for (int i = 0; i < length; i++) {
      buffer.append("<li>");
      buffer.append(fields[i]);
      buffer.append(" = ");
      buffer.append(Entities.encode(values[i]));
      buffer.append("</li>\n");
    }
    buffer.append("</ul>\n");
    return buffer.toString();
  }
  


}

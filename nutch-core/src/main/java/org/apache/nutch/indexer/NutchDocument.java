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
package org.apache.nutch.indexer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.nutch.metadata.Metadata;

/** A {@link NutchDocument} is the unit of indexing. */
public class NutchDocument implements Writable,
    Iterable<Entry<String, NutchField>> {

  public static final byte VERSION = 2;

  private Map<String, NutchField> fields;

  private Metadata documentMeta;

  private float weight;

  public NutchDocument() {
    fields = new HashMap<String, NutchField>();
    documentMeta = new Metadata();
    weight = 1.0f;
  }

  public void add(String name, Object value) {
    NutchField field = fields.get(name);
    if (field == null) {
      field = new NutchField(value);
      fields.put(name, field);
    } else {
      field.add(value);
    }
  }

  public Object getFieldValue(String name) {
    NutchField field = fields.get(name);
    if (field == null) {
      return null;
    }
    if (field.getValues().size() == 0) {
      return null;
    }
    return field.getValues().get(0);
  }

  public NutchField getField(String name) {
    return fields.get(name);
  }

  public NutchField removeField(String name) {
    return fields.remove(name);
  }

  public Collection<String> getFieldNames() {
    return fields.keySet();
  }

  /** Iterate over all fields. */
  public Iterator<Entry<String, NutchField>> iterator() {
    return fields.entrySet().iterator();
  }

  public float getWeight() {
    return weight;
  }

  public void setWeight(float weight) {
    this.weight = weight;
  }

  public Metadata getDocumentMeta() {
    return documentMeta;
  }

  public void readFields(DataInput in) throws IOException {
    fields.clear();
    byte version = in.readByte();
    if (version != VERSION) {
      throw new VersionMismatchException(VERSION, version);
    }
    int size = WritableUtils.readVInt(in);
    for (int i = 0; i < size; i++) {
      String name = Text.readString(in);
      NutchField field = new NutchField();
      field.readFields(in);
      fields.put(name, field);
    }
    weight = in.readFloat();
    documentMeta.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    out.writeByte(VERSION);
    WritableUtils.writeVInt(out, fields.size());
    for (Map.Entry<String, NutchField> entry : fields.entrySet()) {
      Text.writeString(out, entry.getKey());
      NutchField field = entry.getValue();
      field.write(out);
    }
    out.writeFloat(weight);
    documentMeta.write(out);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("doc {\n");
    for (Map.Entry<String, NutchField> entry : fields.entrySet()) {
      sb.append("\t");
      sb.append(entry.getKey());
      sb.append(":\t");
      sb.append(entry.getValue());
      sb.append("\n");
    }
    sb.append("}\n");
    return sb.toString();
  }
}

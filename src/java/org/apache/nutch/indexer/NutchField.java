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

package org.apache.nutch.indexer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * This class represents a multi-valued field with a weight. Values are
 * arbitrary objects.
 */
public class NutchField implements Writable {
  private float weight;
  private List<Object> values = new ArrayList<>();

  public NutchField() {
    //default constructor
  }

  public NutchField(Object value) {
    this(value, 1.0f);
  }

  public NutchField(Object value, float weight) {
    this.weight = weight;
    if (value instanceof Collection) {
      values.addAll((Collection<?>) value);
    } else {
      values.add(value);
    }
  }

  public void add(Object value) {
    values.add(value);
  }

  public float getWeight() {
    return weight;
  }

  public void setWeight(float weight) {
    this.weight = weight;
  }

  public List<Object> getValues() {
    return values;
  }

  public void reset() {
    weight = 1.0f;
    values.clear();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    NutchField result = (NutchField) super.clone();
    result.weight = weight;
    result.values = values;

    return result;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    weight = in.readFloat();
    int count = in.readInt();
    values = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String type = Text.readString(in);

      if ("java.lang.String".equals(type)) {
        values.add(Text.readString(in));
      } else if ("java.lang.Boolean".equals(type)) {
        values.add(in.readBoolean());
      } else if ("java.lang.Integer".equals(type)) {
        values.add(in.readInt());
      } else if ("java.lang.Float".equals(type)) {
        values.add(in.readFloat());
      } else if ("java.lang.Long".equals(type)) {
        values.add(in.readLong());
      } else if ("java.util.Date".equals(type)) {
        values.add(new Date(in.readLong()));
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(weight);
    out.writeInt(values.size());
    for (Object value : values) {

      Text.writeString(out, value.getClass().getName());

      if (value instanceof Boolean) {
        out.writeBoolean((Boolean) value);
      } else if (value instanceof Integer) {
        out.writeInt((Integer) value);
      } else if (value instanceof Long) {
        out.writeLong((Long) value);
      } else if (value instanceof Float) {
        out.writeFloat((Float) value);
      } else if (value instanceof String) {
        Text.writeString(out, (String) value);
      } else if (value instanceof Date) {
        Date date = (Date) value;
        out.writeLong(date.getTime());
      }
    }
  }

  @Override
  public String toString() {
    return values.toString();
  }

}

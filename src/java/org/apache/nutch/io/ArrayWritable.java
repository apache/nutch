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

package org.apache.nutch.io;

import java.io.*;
import java.lang.reflect.Array;

/** A Writable for arrays containing instances of a class. */
public class ArrayWritable implements Writable {
  private Class valueClass;
  private Writable[] values;

  public ArrayWritable() {
    this.valueClass = null;
  }

  public ArrayWritable(Class valueClass) {
    this.valueClass = valueClass;
  }

  public ArrayWritable(Class valueClass, Writable[] values) {
    this(valueClass);
    this.values = values;
  }

  public ArrayWritable(String[] strings) {
    this(UTF8.class, new Writable[strings.length]);
    for (int i = 0; i < strings.length; i++) {
      values[i] = new UTF8(strings[i]);
    }
  }

  public void setValueClass(Class valueClass) {
    if (valueClass != this.valueClass) {
        this.valueClass = valueClass;
        this.values = null;
    }
  }
  
  public Class getValueClass() {
    return valueClass;
  }

  public String[] toStrings() {
    String[] strings = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      strings[i] = values[i].toString();
    }
    return strings;
  }

  public Object toArray() {
    Object result = Array.newInstance(valueClass, values.length);
    for (int i = 0; i < values.length; i++) {
      Array.set(result, i, values[i]);
    }
    return result;
  }

  public void set(Writable[] values) { this.values = values; }

  public Writable[] get() { return values; }

  public void readFields(DataInput in) throws IOException {
    values = new Writable[in.readInt()];          // construct values
    for (int i = 0; i < values.length; i++) {
      Writable value;                             // construct value
      try {
        value = (Writable)valueClass.newInstance();
      } catch (InstantiationException e) {
        throw new RuntimeException(e.toString());
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e.toString());
      }
      value.readFields(in);                       // read a value
      values[i] = value;                          // store it in values
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(values.length);                 // write values
    for (int i = 0; i < values.length; i++) {
      values[i].write(out);
    }
  }

}


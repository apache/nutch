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
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * This class represents a multi-valued field with a weight. Values are arbitrary
 * objects.
 */
public class NutchField implements Writable {
  private float weight;
  private List<Object> values = new ArrayList<Object>();
  
  public NutchField() {
    
  }
  
  public NutchField(Object value) {
    this(value, 1.0f);
  }
  
  public NutchField(Object value, float weight) {
    this.weight = weight;
    if (value instanceof Collection) {
      values.addAll((Collection<Object>)value);
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

  public void readFields(DataInput in) throws IOException {
  }

  public void write(DataOutput out) throws IOException {
  }

}

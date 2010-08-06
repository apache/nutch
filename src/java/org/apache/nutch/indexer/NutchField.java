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

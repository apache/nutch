package org.apache.nutch.indexer.field;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * A class that holds a grouping of FieldWritable objects.
 */
public class FieldsWritable
  implements Writable {

  private List<FieldWritable> fieldsList = new ArrayList<FieldWritable>();

  public FieldsWritable() {

  }
  
  public boolean hasField(String name) {
    for (FieldWritable field : fieldsList) {
      if (field.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }
  
  public FieldWritable getField(String name) {
    for (FieldWritable field : fieldsList) {
      if (field.getName().equals(name)) {
        return field;
      }
    }
    return null;
  }
  
  public List<FieldWritable> getFields(String name) {
    List<FieldWritable> named = new ArrayList<FieldWritable>();
    for (FieldWritable field : fieldsList) {
      if (field.getName().equals(name)) {
        named.add(field);
      }
    }
    return named.size() > 0 ? named : null;
  }
  
  public List<FieldWritable> getFieldsList() {
    return fieldsList;
  }

  public void setFieldsList(List<FieldWritable> fieldsList) {
    this.fieldsList = fieldsList;
  }

  public void readFields(DataInput in)
    throws IOException {
    fieldsList.clear();
    int numFields = in.readInt();
    for (int i = 0; i < numFields; i++) {
      FieldWritable field = new FieldWritable();
      field.readFields(in);
      fieldsList.add(field);
    }
  }

  public void write(DataOutput out)
    throws IOException {
    int numFields = fieldsList.size();
    out.writeInt(numFields);
    for (int i = 0; i < numFields; i++) {
      fieldsList.get(i).write(out);
    }
  }

}

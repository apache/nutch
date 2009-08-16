package org.apache.nutch.util.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class ColumnData implements Writable {

  private byte[] data;
  private boolean isModified;

  public ColumnData() { // do not use!
  }
  
  public ColumnData(byte[] data) {
    this(data, false);
  }
  
  public ColumnData(byte[] data, boolean isModified) {
    this.data = data;
    this.isModified = isModified;
  }
  
  public void set(byte[] data) {
    this.data = data;
    isModified = true;
  }
  
  public byte[] getData() {
    return data;
  }

  public boolean isModified() {
    return isModified;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    data = Bytes.readByteArray(in);
    isModified = in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, data);
    out.writeBoolean(isModified);
  }
}
